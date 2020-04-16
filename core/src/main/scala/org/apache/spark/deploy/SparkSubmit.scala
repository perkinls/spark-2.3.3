/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy

import java.io._
import java.lang.reflect.{InvocationTargetException, Modifier, UndeclaredThrowableException}
import java.net.URL
import java.security.PrivilegedExceptionAction
import java.text.ParseException
import java.util.UUID

import scala.annotation.tailrec
import scala.collection.mutable.{ArrayBuffer, HashMap, Map}
import scala.util.{Properties, Try}

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.ivy.Ivy
import org.apache.ivy.core.LogOptions
import org.apache.ivy.core.module.descriptor._
import org.apache.ivy.core.module.id.{ArtifactId, ModuleId, ModuleRevisionId}
import org.apache.ivy.core.report.ResolveReport
import org.apache.ivy.core.resolve.ResolveOptions
import org.apache.ivy.core.retrieve.RetrieveOptions
import org.apache.ivy.core.settings.IvySettings
import org.apache.ivy.plugins.matcher.GlobPatternMatcher
import org.apache.ivy.plugins.repository.file.FileRepository
import org.apache.ivy.plugins.resolver.{ChainResolver, FileSystemResolver, IBiblioResolver}

import org.apache.spark._
import org.apache.spark.api.r.RUtils
import org.apache.spark.deploy.rest._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.util._

/**
 * 这个类主要是提交app,终止和请求状态,但目前终止和请求只能在standalone和mesos模式下
 */
private[deploy] object SparkSubmitAction extends Enumeration {
  type SparkSubmitAction = Value
  val SUBMIT, KILL, REQUEST_STATUS = Value
}


/**
 * 主要是通过网关去启动一个Spark应用程序。
 * 这个程序提供了Spark支持的处理设置类路径依赖和相关的Spark提供了一层在不同的集群管理和部署模式
 */
object SparkSubmit extends CommandLineUtils with Logging {

  import DependencyUtils._

  // Cluster managers 集群模式
  private val YARN = 1
  private val STANDALONE = 2
  private val MESOS = 4
  private val LOCAL = 8
  private val KUBERNETES = 16
  private val ALL_CLUSTER_MGRS = YARN | STANDALONE | MESOS | LOCAL | KUBERNETES

  // Deploy modes 部署模式
  private val CLIENT = 1
  private val CLUSTER = 2
  private val ALL_DEPLOY_MODES = CLIENT | CLUSTER

  // shell命令相关的常量
  private val SPARK_SHELL = "spark-shell"
  private val PYSPARK_SHELL = "pyspark-shell"
  private val SPARKR_SHELL = "sparkr-shell"
  private val SPARKR_PACKAGE_ARCHIVE = "sparkr.zip"
  private val R_PACKAGE_ARCHIVE = "rpkg.zip"

  // 找不到类的错误码
  private val CLASS_NOT_FOUND_EXIT_STATUS = 101

  // 测试用的常量
  private[deploy] val YARN_CLUSTER_SUBMIT_CLASS =
    "org.apache.spark.deploy.yarn.YarnClusterApplication"
  private[deploy] val REST_CLUSTER_SUBMIT_CLASS = classOf[RestSubmissionClientApp].getName()
  private[deploy] val STANDALONE_CLUSTER_SUBMIT_CLASS = classOf[ClientApp].getName()
  private[deploy] val KUBERNETES_CLUSTER_SUBMIT_CLASS =
    "org.apache.spark.deploy.k8s.submit.KubernetesClientApplication"

  // scalastyle:off println
  private[spark] def printVersionAndExit(): Unit = {
    printStream.println(
      """Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version %s
      /_/
                        """.format(SPARK_VERSION))
    printStream.println("Using Scala %s, %s, %s".format(
      Properties.versionString, Properties.javaVmName, Properties.javaVersion))
    printStream.println("Branch %s".format(SPARK_BRANCH))
    printStream.println("Compiled by user %s on %s".format(SPARK_BUILD_USER, SPARK_BUILD_DATE))
    printStream.println("Revision %s".format(SPARK_REVISION))
    printStream.println("Url %s".format(SPARK_REPO_URL))
    printStream.println("Type --help for more information.")
    exitFn(0)
  }


  override def main(args: Array[String]): Unit = {
    // 初始化logging系统,并跟日志判断是否需要在app启动时重启
    val uninitLog = initializeLogIfNecessary(true, silent = true)

    /**
     * 构建spark提交需要的参数并进行赋值 SparkSubmitArguments
     * 1.解析参数
     * 2.从属性文件填充“sparkProperties”映射（未指定默认情况下未：spark-defaults.conf）
     * 3.移除不是以"spark." 开头的变量
     * 4.参数填充对应到实体属性上
     * 5.action参数验证
     */
    val appArgs = new SparkSubmitArguments(args)

    // 参数不重复则输出配置
    if (appArgs.verbose) {
      printStream.println(appArgs)
    }
    appArgs.action match {
      case SparkSubmitAction.SUBMIT => submit(appArgs, uninitLog)
      case SparkSubmitAction.KILL => kill(appArgs)
      case SparkSubmitAction.REQUEST_STATUS => requestStatus(appArgs)
    }
  }

  /**
   * 杀死一个已经存在的submission使用REST协议，仅仅在Standalone和Mesos模式下可用
   */
  private def kill(args: SparkSubmitArguments): Unit = {
    new RestSubmissionClient(args.master)
      .killSubmission(args.submissionToKill)
  }

  /**
   * 请求一个存在的submission状态 仅仅在Standalone和Mesos模式下
   */
  private def requestStatus(args: SparkSubmitArguments): Unit = {
    new RestSubmissionClient(args.master)
      .requestSubmissionStatus(args.submissionToRequestStatusFor)
  }


  /**
   * 通过匹配SUBMIT执行的submit()
   *
   * 首先是根据不同调度模式和yarn不同模式,导入调用类的路径,默认配置及输入参数,准备相应的启动环境
   * 然后通过对应的环境来调用相应子类的main方法
   * 这里因为涉及到重复调用,所以采用了@tailrec尾递归,即重复调用方法的最后一句并返回结果
   * 即:runMain(childArgs, childClasspath, sparkConf, childMainClass, args.verbose)
   */
  @tailrec
  private def submit(args: SparkSubmitArguments, uninitLog: Boolean): Unit = {

    /**
     * 先准备运行环境,传入解析的各种参数
     * 这里会先进入
     * lazy val secMgr = new SecurityManager(sparkConf)
     * 先初始化SecurityManager后,再进入prepareSubmitEnvironment()
     * prepareSubmitEnvironment()代码比较长,放到最下面去解析
     */
    val (childArgs, childClasspath, sparkConf, childMainClass) = prepareSubmitEnvironment(args)

    // 主要是调用runMain()启动相应环境的main()的方法
    // 环境准备好以后,会先往下运行判断,这里是在等着调用
    def doRunMain(): Unit = {
      // 提交时可以指定--proxy-user,如果没有指定,则获取当前用户
      if (args.proxyUser != null) {
        val proxyUser = UserGroupInformation.createProxyUser(args.proxyUser,
          UserGroupInformation.getCurrentUser())
        try {
          proxyUser.doAs(new PrivilegedExceptionAction[Unit]() {
            override def run(): Unit = {
              // 这里是真正的执行,runMain()
              runMain(childArgs, childClasspath, sparkConf, childMainClass, args.verbose)
            }
          })
        } catch {
          case e: Exception =>
            // Hadoop's AuthorizationException suppresses the exception's stack trace, which
            // makes the message printed to the output by the JVM not very helpful. Instead,
            // detect exceptions with empty stack traces here, and treat them differently.
            if (e.getStackTrace().length == 0) {
              // scalastyle:off println
              printStream.println(s"ERROR: ${e.getClass().getName()}: ${e.getMessage()}")
              // scalastyle:on println
              exitFn(1)
            } else {
              throw e
            }
        }
      } else {
        // 没有指定用户时执行
        runMain(childArgs, childClasspath, sparkConf, childMainClass, args.verbose)
      }
    }

    // 启动main后重新初始化logging
    if (uninitLog) {
      Logging.uninitialize()
    }

    // standalone模式有两种提交网关,
    // （1）使用o.a.s.apply.client作为包装器的传统RPC网关和基于REST服务的网关
    // （2）spark1.3后默认使用REST
    // 如果master终端没有使用REST服务,spark会故障切换到RPC 这里判断standalone模式和使用REST服务
    if (args.isStandaloneCluster && args.useRest) {
      // 异常捕获,判断正确的话输出信息,进入doRunMain()
      try {
        logInfo("Running Spark using the REST application submission protocol.")
        doRunMain()
      } catch {

        // Fail over to use the legacy submission gateway
        // 否则异常输出信息,并设置submit失败
        case e: SubmitRestConnectionException =>
          logWarning(s"Master endpoint ${args.master} was not a REST server. " +
            "Falling back to legacy submission gateway instead.")
          args.useRest = false
          submit(args, false)
      }

      // In all other modes, just run the main class as prepared
      // 其他模式,按准备的环境调用上面的doRunMain()运行相应的main()
      // 在进入前,初始化了SparkContext和SparkSession
    } else {
      doRunMain()
    }
  }

  /**
   * 准备各种模式的配置参数
   *
   * @param args 用于环境准备的已分析SparkSubmitArguments
   * @param conf 在Hadoop配置中，仅在单元测试中设置此参数。
   * @return a 4-tuple:
   *         (1) the arguments for the child process,
   *         (2) a list of classpath entries for the child,
   *         (3) a map of system properties, and
   *         (4) the main class for the child
   *         返回一个4元组(childArgs, childClasspath, sparkConf, childMainClass)
   *         childArgs：子进程的参数
   *         childClasspath：子级的类路径条目列表
   *         sparkConf：系统参数map集合
   *         childMainClass：子级的主类
   *
   *         Exposed for testing.
   *
   *         由于不同的部署方式其卖弄函数是不一样的，主要是由spark的提交参数决定
   */
  private[deploy] def prepareSubmitEnvironment(
                                                args: SparkSubmitArguments,
                                                conf: Option[HadoopConfiguration] = None)
  : (Seq[String], Seq[String], SparkConf, String) = {
    try {
      doPrepareSubmitEnvironment(args, conf)
    } catch {
      case e: SparkException =>
        printErrorAndExit(e.getMessage)
        throw e
    }
  }

  private def doPrepareSubmitEnvironment(
                                          args: SparkSubmitArguments,
                                          conf: Option[HadoopConfiguration] = None)
  : (Seq[String], Seq[String], SparkConf, String) = {
    // Return values
    val childArgs = new ArrayBuffer[String]()
    val childClasspath = new ArrayBuffer[String]()
    // SparkConf 会默认加一些系统参数
    val sparkConf = new SparkConf()
    var childMainClass = ""

    // 设置集群模式
    // 也就是提交时指定--master local/yarn/yarn-client/yarn-cluster/spark://192.168.2.1:7077或者 mesos,k8s等运行模式
    val clusterManager: Int = args.master match {
      case "yarn" => YARN
      case "yarn-client" | "yarn-cluster" =>
        printWarning(s"Master ${args.master} is deprecated since 2.0." +
          " Please use master \"yarn\" with specified deploy mode instead.")
        YARN
      case m if m.startsWith("spark") => STANDALONE
      case m if m.startsWith("mesos") => MESOS
      case m if m.startsWith("k8s") => KUBERNETES
      case m if m.startsWith("local") => LOCAL
      case _ =>
        printErrorAndExit("Master must either be yarn or start with spark, mesos, k8s, or local")
        -1
    }

    // 设置部署模式 --deploy-mode
    var deployMode: Int = args.deployMode match {
      case "client" | null => CLIENT
      case "cluster" => CLUSTER
      case _ => printErrorAndExit("Deploy mode must be either client or cluster"); -1
    }

    //由于指定“yarn-cluster”和“yarn-client”的不受支持的方式封装了主模式和部署模式，
    // 因此我们有一些逻辑来推断master和部署模式(如果只指定一种模式)，或者在它们不一致时提前退出
    if (clusterManager == YARN) {
      (args.master, args.deployMode) match {
        case ("yarn-cluster", null) =>
          deployMode = CLUSTER
          args.master = "yarn"
        case ("yarn-cluster", "client") =>
          printErrorAndExit("Client deploy mode is not compatible with master \"yarn-cluster\"")
        case ("yarn-client", "cluster") =>
          printErrorAndExit("Cluster deploy mode is not compatible with master \"yarn-client\"")
        case (_, mode) =>
          args.master = "yarn"
      }

      // Make sure YARN is included in our build if we're trying to use it
      if (!Utils.classIsLoadable(YARN_CLUSTER_SUBMIT_CLASS) && !Utils.isTesting) {
        printErrorAndExit(
          "Could not load YARN classes. " +
            "This copy of Spark may not have been compiled with YARN support.")
      }
    }

    // 判断k8s模式master和非testing模式
    if (clusterManager == KUBERNETES) {
      args.master = Utils.checkAndGetK8sMasterUrl(args.master)
      // Make sure KUBERNETES is included in our build if we're trying to use it
      if (!Utils.classIsLoadable(KUBERNETES_CLUSTER_SUBMIT_CLASS) && !Utils.isTesting) {
        printErrorAndExit(
          "Could not load KUBERNETES classes. " +
            "This copy of Spark may not have been compiled with KUBERNETES support.")
      }
    }

    // 错判断不可用模式
    (clusterManager, deployMode) match {
      case (STANDALONE, CLUSTER) if args.isPython =>
        printErrorAndExit("Cluster deploy mode is currently not supported for python " +
          "applications on standalone clusters.")
      case (STANDALONE, CLUSTER) if args.isR =>
        printErrorAndExit("Cluster deploy mode is currently not supported for R " +
          "applications on standalone clusters.")
      case (KUBERNETES, _) if args.isPython =>
        printErrorAndExit("Python applications are currently not supported for Kubernetes.")
      case (KUBERNETES, _) if args.isR =>
        printErrorAndExit("R applications are currently not supported for Kubernetes.")
      case (KUBERNETES, CLIENT) =>
        printErrorAndExit("Client mode is currently not supported for Kubernetes.")
      case (LOCAL, CLUSTER) =>
        printErrorAndExit("Cluster deploy mode is not compatible with master \"local\"")
      case (_, CLUSTER) if isShell(args.primaryResource) =>
        printErrorAndExit("Cluster deploy mode is not applicable to Spark shells.")
      case (_, CLUSTER) if isSqlShell(args.mainClass) =>
        printErrorAndExit("Cluster deploy mode is not applicable to Spark SQL shell.")
      case (_, CLUSTER) if isThriftServer(args.mainClass) =>
        printErrorAndExit("Cluster deploy mode is not applicable to Spark Thrift server.")
      case _ =>
    }

    // args.deployMode为空则设置deployMode值为参数,因为上面判断了args.deployMode为空deployMode为client
    (args.deployMode, deployMode) match {
      case (null, CLIENT) => args.deployMode = "client"
      case (null, CLUSTER) => args.deployMode = "cluster"
      case _ =>
    }
    // 根据资源管理器和部署模式，进行逻辑判断出几种特殊运行方式。
    val isYarnCluster = clusterManager == YARN && deployMode == CLUSTER
    val isMesosCluster = clusterManager == MESOS && deployMode == CLUSTER
    val isStandAloneCluster = clusterManager == STANDALONE && deployMode == CLUSTER
    val isKubernetesCluster = clusterManager == KUBERNETES && deployMode == CLUSTER

    // 这里主要是添加相关的依赖
    if (!isMesosCluster && !isStandAloneCluster) {
      // 如果有maven依赖项，则解析它们，并将类路径添加到jar中。对于包含Python代码的包，也将它们添加到py文件中
      val resolvedMavenCoordinates = DependencyUtils.resolveMavenDependencies(
        args.packagesExclusions, args.packages, args.repositories, args.ivyRepoPath,
        args.ivySettingsPath)

      if (!StringUtils.isBlank(resolvedMavenCoordinates)) {
        args.jars = mergeFileLists(args.jars, resolvedMavenCoordinates)
        if (args.isPython || isInternal(args.primaryResource)) {
          args.pyFiles = mergeFileLists(args.pyFiles, resolvedMavenCoordinates)
        }
      }
      // 安装任何可能通过--jar或--packages传递的R包。Spark包可能在jar中包含R源代码。
      if (args.isR && !StringUtils.isBlank(args.jars)) {
        RPackageUtils.checkAndBuildRPackage(args.jars, printStream, args.verbose)
      }
    }

    args.sparkProperties.foreach { case (k, v) => sparkConf.set(k, v) }
    // sparkConf 加载Hadoop相关配置文件
    val hadoopConf = conf.getOrElse(SparkHadoopUtil.newConfiguration(sparkConf))
    // 工作临时目录
    val targetDir = Utils.createTempDir()

    //  判断当前模式下sparkConf的k/v键值对中key是否在JVM中全局可用
    // 确保keytab在JVM中的任何位置都可用（keytab是Kerberos的身份认证,详情可参考：http://ftuto.lofter.com/post/31e97f_6ad659f）
    if (clusterManager == YARN || clusterManager == LOCAL || clusterManager == MESOS) {
      // 当前运行环境的用户不为空,args中yarn模式参数key列表不为空,则提示key列表文件不存在
      if (args.principal != null) {
        if (args.keytab != null) {
          require(new File(args.keytab).exists(), s"Keytab file: ${args.keytab} does not exist")
          // 在sysProps中添加keytab和主体配置，以供以后使用;例如，在spark sql中，用于与HiveMetastore对话的隔离类装入器将使用这些设置。
          // 它们将被设置为Java系统属性，然后由SparkConf加载
          sparkConf.set(KEYTAB, args.keytab)
          sparkConf.set(PRINCIPAL, args.principal)
          UserGroupInformation.loginUserFromKeytab(args.principal, args.keytab)
        }
      }
    }

    // Resolve glob path for different resources.
    // 设置全局资源,也就是合并各种模式依赖的路径的资源和hadoopConf中设置路径的资源，各种jars,file,pyfile和压缩包
    args.jars = Option(args.jars).map(resolveGlobPaths(_, hadoopConf)).orNull
    args.files = Option(args.files).map(resolveGlobPaths(_, hadoopConf)).orNull
    args.pyFiles = Option(args.pyFiles).map(resolveGlobPaths(_, hadoopConf)).orNull
    args.archives = Option(args.archives).map(resolveGlobPaths(_, hadoopConf)).orNull

    // 创建SecurityManager实例
    lazy val secMgr = new SecurityManager(sparkConf)

    // 在Client模式下，下载远程资源文件。
    var localPrimaryResource: String = null
    var localJars: String = null
    var localPyFiles: String = null
    if (deployMode == CLIENT) {
      localPrimaryResource = Option(args.primaryResource).map {
        downloadFile(_, targetDir, sparkConf, hadoopConf, secMgr)
      }.orNull
      localJars = Option(args.jars).map {
        downloadFileList(_, targetDir, sparkConf, hadoopConf, secMgr)
      }.orNull
      localPyFiles = Option(args.pyFiles).map {
        downloadFileList(_, targetDir, sparkConf, hadoopConf, secMgr)
      }.orNull
    }

    // When running in YARN, for some remote resources with scheme:
    //   1. Hadoop FileSystem doesn't support them.
    //   2. We explicitly bypass Hadoop FileSystem with "spark.yarn.dist.forceDownloadSchemes".
    // We will download them to local disk prior to add to YARN's distributed cache.
    // For yarn client mode, since we already download them with above code, so we only need to
    // figure out the local path and replace the remote one.
    // yarn模式下,hdfs不支持加载到内存,所以采用"spark.yarn.dist.forceDownloadSchemes"方案(在添加到YARN分布式缓存之前，文件将被下载到本地磁盘的逗号分隔列表。用于YARN服务不支持Spark支持的方案的情况)
    // 所以先把方案列表文件下载到本地,再通过相应方案加载资源到分布式内存中
    // 在yarn-client模式中,上面的代码中已经把远程文件下载到了本地,只需要获取本地路径替换掉远程路径即可
    if (clusterManager == YARN) {
      // 加载方案列表
      val forceDownloadSchemes = sparkConf.get(FORCE_DOWNLOAD_SCHEMES)

      // 判断是否需要下载的方法
      def shouldDownload(scheme: String): Boolean = {
        forceDownloadSchemes.contains("*") || forceDownloadSchemes.contains(scheme) ||
          Try {
            FileSystem.getFileSystemClass(scheme, hadoopConf)
          }.isFailure
      }

      // 下载资源的方法
      def downloadResource(resource: String): String = {
        val uri = Utils.resolveURI(resource)
        uri.getScheme match {
          case "local" | "file" => resource
          case e if shouldDownload(e) =>
            val file = new File(targetDir, new Path(uri).getName)
            if (file.exists()) {
              file.toURI.toString
            } else {
              downloadFile(resource, targetDir, sparkConf, hadoopConf, secMgr)
            }
          case _ => uri.toString
        }
      }

      // 下载主要运行资源
      args.primaryResource = Option(args.primaryResource).map {
        downloadResource
      }.orNull
      // 下载文件
      args.files = Option(args.files).map { files =>
        Utils.stringToSeq(files).map(downloadResource).mkString(",")
      }.orNull
      args.pyFiles = Option(args.pyFiles).map { pyFiles =>
        Utils.stringToSeq(pyFiles).map(downloadResource).mkString(",")
      }.orNull
      // 下载jars
      args.jars = Option(args.jars).map { jars =>
        Utils.stringToSeq(jars).map(downloadResource).mkString(",")
      }.orNull
      // 下载压缩文件
      args.archives = Option(args.archives).map { archives =>
        Utils.stringToSeq(archives).map(downloadResource).mkString(",")
      }.orNull
    }

    // 如果我们正在运行python应用，请将主类设置为特定的python运行器
    if (args.isPython && deployMode == CLIENT) {
      if (args.primaryResource == PYSPARK_SHELL) {
        args.mainClass = "org.apache.spark.api.python.PythonGatewayServer"
      } else {
        // If a python file is provided, add it to the child arguments and list of files to deploy.
        // Usage: PythonAppRunner <main python file> <extra python files> [app arguments]
        args.mainClass = "org.apache.spark.deploy.PythonRunner"
        args.childArgs = ArrayBuffer(localPrimaryResource, localPyFiles) ++ args.childArgs
        if (clusterManager != YARN) {
          // The YARN backend distributes the primary file differently, so don't merge it.
          args.files = mergeFileLists(args.files, args.primaryResource)
        }
      }
      if (clusterManager != YARN) {
        // The YARN backend handles python files differently, so don't merge the lists.
        args.files = mergeFileLists(args.files, args.pyFiles)
      }
      if (localPyFiles != null) {
        sparkConf.set("spark.submit.pyFiles", localPyFiles)
      }
    }

    // 在R应用程序的yarn模式中，添加SparkR包存档和包含所有构建的R库的R包存档到存档中，以便它们可以随作业一起分发
    if (args.isR && clusterManager == YARN) {
      val sparkRPackagePath = RUtils.localSparkRPackagePath
      if (sparkRPackagePath.isEmpty) {
        printErrorAndExit("SPARK_HOME does not exist for R application in YARN mode.")
      }
      val sparkRPackageFile = new File(sparkRPackagePath.get, SPARKR_PACKAGE_ARCHIVE)
      if (!sparkRPackageFile.exists()) {
        printErrorAndExit(s"$SPARKR_PACKAGE_ARCHIVE does not exist for R application in YARN mode.")
      }
      val sparkRPackageURI = Utils.resolveURI(sparkRPackageFile.getAbsolutePath).toString

      // Distribute the SparkR package.
      // Assigns a symbol link name "sparkr" to the shipped package.
      args.archives = mergeFileLists(args.archives, sparkRPackageURI + "#sparkr")

      // Distribute the R package archive containing all the built R packages.
      if (!RUtils.rPackages.isEmpty) {
        val rPackageFile =
          RPackageUtils.zipRLibraries(new File(RUtils.rPackages.get), R_PACKAGE_ARCHIVE)
        if (!rPackageFile.exists()) {
          printErrorAndExit("Failed to zip all the built R packages.")
        }

        val rPackageURI = Utils.resolveURI(rPackageFile.getAbsolutePath).toString
        // Assigns a symbol link name "rpkg" to the shipped package.
        args.archives = mergeFileLists(args.archives, rPackageURI + "#rpkg")
      }
    }

    // TODO: Support distributing R packages with standalone cluster
    if (args.isR && clusterManager == STANDALONE && !RUtils.rPackages.isEmpty) {
      printErrorAndExit("Distributing R packages with standalone cluster is not supported.")
    }

    // TODO: Support distributing R packages with mesos cluster
    if (args.isR && clusterManager == MESOS && !RUtils.rPackages.isEmpty) {
      printErrorAndExit("Distributing R packages with mesos cluster is not supported.")
    }

    // 如果我们正在运行R应用，请将主类设置为特定的R运行器
    if (args.isR && deployMode == CLIENT) {
      if (args.primaryResource == SPARKR_SHELL) {
        args.mainClass = "org.apache.spark.api.r.RBackend"
      } else {
        // If an R file is provided, add it to the child arguments and list of files to deploy.
        // Usage: RRunner <main R file> [app arguments]
        args.mainClass = "org.apache.spark.deploy.RRunner"
        args.childArgs = ArrayBuffer(localPrimaryResource) ++ args.childArgs
        args.files = mergeFileLists(args.files, args.primaryResource)
      }
    }

    if (isYarnCluster && args.isR) {
      // In yarn-cluster mode for an R app, add primary resource to files
      // that can be distributed with the job
      args.files = mergeFileLists(args.files, args.primaryResource)
    }

    // Special flag to avoid deprecation warnings at the client
    sys.props("SPARK_SUBMIT") = "true"

    //  为各种部署模式设置相应参数这里返回的是元组OptionAssigner类没有方法,只是设置了参数类型
    val options = List[OptionAssigner](

      // All cluster managers
      OptionAssigner(args.master, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES, confKey = "spark.master"),
      OptionAssigner(args.deployMode, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES,
        confKey = "spark.submit.deployMode"),
      OptionAssigner(args.name, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES, confKey = "spark.app.name"),
      OptionAssigner(args.ivyRepoPath, ALL_CLUSTER_MGRS, CLIENT, confKey = "spark.jars.ivy"),
      OptionAssigner(args.driverMemory, ALL_CLUSTER_MGRS, CLIENT,
        confKey = "spark.driver.memory"),
      OptionAssigner(args.driverExtraClassPath, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES,
        confKey = "spark.driver.extraClassPath"),
      OptionAssigner(args.driverExtraJavaOptions, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES,
        confKey = "spark.driver.extraJavaOptions"),
      OptionAssigner(args.driverExtraLibraryPath, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES,
        confKey = "spark.driver.extraLibraryPath"),

      // Propagate attributes for dependency resolution at the driver side
      OptionAssigner(args.packages, STANDALONE | MESOS, CLUSTER, confKey = "spark.jars.packages"),
      OptionAssigner(args.repositories, STANDALONE | MESOS, CLUSTER,
        confKey = "spark.jars.repositories"),
      OptionAssigner(args.ivyRepoPath, STANDALONE | MESOS, CLUSTER, confKey = "spark.jars.ivy"),
      OptionAssigner(args.packagesExclusions, STANDALONE | MESOS,
        CLUSTER, confKey = "spark.jars.excludes"),

      // Yarn only
      OptionAssigner(args.queue, YARN, ALL_DEPLOY_MODES, confKey = "spark.yarn.queue"),
      OptionAssigner(args.numExecutors, YARN, ALL_DEPLOY_MODES,
        confKey = "spark.executor.instances"),
      OptionAssigner(args.pyFiles, YARN, ALL_DEPLOY_MODES, confKey = "spark.yarn.dist.pyFiles"),
      OptionAssigner(args.jars, YARN, ALL_DEPLOY_MODES, confKey = "spark.yarn.dist.jars"),
      OptionAssigner(args.files, YARN, ALL_DEPLOY_MODES, confKey = "spark.yarn.dist.files"),
      OptionAssigner(args.archives, YARN, ALL_DEPLOY_MODES, confKey = "spark.yarn.dist.archives"),
      OptionAssigner(args.principal, YARN, ALL_DEPLOY_MODES, confKey = "spark.yarn.principal"),
      OptionAssigner(args.keytab, YARN, ALL_DEPLOY_MODES, confKey = "spark.yarn.keytab"),

      // Other options
      OptionAssigner(args.executorCores, STANDALONE | YARN | KUBERNETES, ALL_DEPLOY_MODES,
        confKey = "spark.executor.cores"),
      OptionAssigner(args.executorMemory, STANDALONE | MESOS | YARN | KUBERNETES, ALL_DEPLOY_MODES,
        confKey = "spark.executor.memory"),
      OptionAssigner(args.totalExecutorCores, STANDALONE | MESOS | KUBERNETES, ALL_DEPLOY_MODES,
        confKey = "spark.cores.max"),
      OptionAssigner(args.files, LOCAL | STANDALONE | MESOS | KUBERNETES, ALL_DEPLOY_MODES,
        confKey = "spark.files"),
      OptionAssigner(args.jars, LOCAL, CLIENT, confKey = "spark.jars"),
      OptionAssigner(args.jars, STANDALONE | MESOS | KUBERNETES, ALL_DEPLOY_MODES,
        confKey = "spark.jars"),
      OptionAssigner(args.driverMemory, STANDALONE | MESOS | YARN | KUBERNETES, CLUSTER,
        confKey = "spark.driver.memory"),
      OptionAssigner(args.driverCores, STANDALONE | MESOS | YARN | KUBERNETES, CLUSTER,
        confKey = "spark.driver.cores"),
      OptionAssigner(args.supervise.toString, STANDALONE | MESOS, CLUSTER,
        confKey = "spark.driver.supervise"),
      OptionAssigner(args.ivyRepoPath, STANDALONE, CLUSTER, confKey = "spark.jars.ivy"),

      // An internal option used only for spark-shell to add user jars to repl's classloader,
      // previously it uses "spark.jars" or "spark.yarn.dist.jars" which now may be pointed to
      // remote jars, so adding a new option to only specify local jars for spark-shell internally.
      OptionAssigner(localJars, ALL_CLUSTER_MGRS, CLIENT, confKey = "spark.repl.local.jars")
    )

    // 在客户端模式下，直接启动应用程序主类
    // 另外，将主应用程序jar和所有添加的jar（如果有）添加到classpath
    if (deployMode == CLIENT) {
      childMainClass = args.mainClass
      if (localPrimaryResource != null && isUserJar(localPrimaryResource)) {
        childClasspath += localPrimaryResource
      }
      if (localJars != null) {
        childClasspath ++= localJars.split(",")
      }
    }

    // 添加主应用程序jar和任何添加到类路径的jar，以yarn客户端需要这些jar。
    // 这里假设primaryResource和user jar都是本地jar，否则它不会被添加到yarn客户端的类路径中。
    if (isYarnCluster) {
      if (isUserJar(args.primaryResource)) {
        childClasspath += args.primaryResource
      }
      if (args.jars != null) {
        childClasspath ++= args.jars.split(",")
      }
    }

    if (deployMode == CLIENT) {
      if (args.childArgs != null) {
        childArgs ++= args.childArgs
      }
    }

    // 将所有参数映射到我们选择的模式的命令行选项或系统属性
    for (opt <- options) {
      if (opt.value != null &&
        (deployMode & opt.deployMode) != 0 &&
        (clusterManager & opt.clusterManager) != 0) {
        if (opt.clOption != null) {
          childArgs += (opt.clOption, opt.value)
        }
        if (opt.confKey != null) {
          sparkConf.set(opt.confKey, opt.value)
        }
      }
    }

    // 如果是shell，默认情况下或用户可以将spark.ui.showConsoleProgress设置为true。
    if (isShell(args.primaryResource) && !sparkConf.contains(UI_SHOW_CONSOLE_PROGRESS)) {
      sparkConf.set(UI_SHOW_CONSOLE_PROGRESS, true)
    }

    // 自动添加应用jar，这样用户就不必在yarn集群模式下调用sc.addJar,
    // jar已经作为python和R文件的“app.jar”分布在每个节点上，主要资源已经作为常规文件分布
    if (!isYarnCluster && !args.isPython && !args.isR) {
      var jars = sparkConf.getOption("spark.jars").map(x => x.split(",").toSeq).getOrElse(Seq.empty)
      if (isUserJar(args.primaryResource)) {
        jars = jars ++ Seq(args.primaryResource)
      }
      sparkConf.set("spark.jars", jars.mkString(","))
    }

    // 在standalone cluster模式下，使用REST客户端提交应用程序(Spark 1.3+)。所有Spark参数都将通过系统属性传递给客户端。
    if (args.isStandaloneCluster) {
      if (args.useRest) {
        childMainClass = REST_CLUSTER_SUBMIT_CLASS
        childArgs += (args.primaryResource, args.mainClass)
      } else {
        // In legacy standalone cluster mode, use Client as a wrapper around the user class
        childMainClass = STANDALONE_CLUSTER_SUBMIT_CLASS
        if (args.supervise) {
          childArgs += "--supervise"
        }
        Option(args.driverMemory).foreach { m => childArgs += ("--memory", m) }
        Option(args.driverCores).foreach { c => childArgs += ("--cores", c) }
        childArgs += "launch"
        childArgs += (args.master, args.primaryResource, args.mainClass)
      }
      if (args.childArgs != null) {
        childArgs ++= args.childArgs
      }
    }

    // 让YARN知道这是一个pyspark应用程序，因此它将分发所需的库。
    if (clusterManager == YARN) {
      if (args.isPython) {
        sparkConf.set("spark.yarn.isPython", "true")
      }
    }

    if (clusterManager == MESOS && UserGroupInformation.isSecurityEnabled) {
      setRMPrincipal(sparkConf)
    }

    // 在yarn-cluster模式下，将yarn.Client用作用户类的包装器
    if (isYarnCluster) {
      childMainClass = YARN_CLUSTER_SUBMIT_CLASS
      if (args.isPython) {
        childArgs += ("--primary-py-file", args.primaryResource)
        childArgs += ("--class", "org.apache.spark.deploy.PythonRunner")
      } else if (args.isR) {
        val mainFile = new Path(args.primaryResource).getName
        childArgs += ("--primary-r-file", mainFile)
        childArgs += ("--class", "org.apache.spark.deploy.RRunner")
      } else {
        if (args.primaryResource != SparkLauncher.NO_RESOURCE) {
          childArgs += ("--jar", args.primaryResource)
        }
        childArgs += ("--class", args.mainClass)
      }
      if (args.childArgs != null) {
        args.childArgs.foreach { arg => childArgs += ("--arg", arg) }
      }
    }

    if (isMesosCluster) {
      assert(args.useRest, "Mesos cluster mode is only supported through the REST submission API")
      childMainClass = REST_CLUSTER_SUBMIT_CLASS
      if (args.isPython) {
        // Second argument is main class
        childArgs += (args.primaryResource, "")
        if (args.pyFiles != null) {
          sparkConf.set("spark.submit.pyFiles", args.pyFiles)
        }
      } else if (args.isR) {
        // Second argument is main class
        childArgs += (args.primaryResource, "")
      } else {
        childArgs += (args.primaryResource, args.mainClass)
      }
      if (args.childArgs != null) {
        childArgs ++= args.childArgs
      }
    }

    if (isKubernetesCluster) {
      childMainClass = KUBERNETES_CLUSTER_SUBMIT_CLASS
      if (args.primaryResource != SparkLauncher.NO_RESOURCE) {
        childArgs ++= Array("--primary-java-resource", args.primaryResource)
      }
      childArgs ++= Array("--main-class", args.mainClass)
      if (args.childArgs != null) {
        args.childArgs.foreach { arg =>
          childArgs += ("--arg", arg)
        }
      }
    }

    // 加载通过--conf和默认属性文件指定的所有属性
    for ((k, v) <- args.sparkProperties) {
      sparkConf.setIfMissing(k, v)
    }

    // 在集群模式下忽略spark.driver.host
    if (deployMode == CLUSTER) {
      sparkConf.remove("spark.driver.host")
    }

    // 解析依赖jars和文件的路径
    val pathConfigs = Seq(
      "spark.jars",
      "spark.files",
      "spark.yarn.dist.files",
      "spark.yarn.dist.archives",
      "spark.yarn.dist.jars")
    // 遍历解析的路径,去除空值和不合理的,并解析为新的URI
    pathConfigs.foreach { config =>
      // 如果存在，用解析的URI替换旧的URI
      sparkConf.getOption(config).foreach { oldValue =>
        sparkConf.set(config, Utils.resolveURIs(oldValue))
      }
    }

    // 清理和格式化python文件的路径
    // 如果默认配置中有设置spark.submit.pyFiles,name--py-files不用添加
    sparkConf.getOption("spark.submit.pyFiles").foreach { pyFiles =>
      val resolvedPyFiles = Utils.resolveURIs(pyFiles)
      val formattedPyFiles = if (!isYarnCluster && !isMesosCluster) {
        PythonRunner.formatPaths(resolvedPyFiles).mkString(",")
      } else {
        // 返回清理和格式化后的python文件路径
        resolvedPyFiles
      }
      sparkConf.set("spark.submit.pyFiles", formattedPyFiles)
    }

    // 最终prepareSubmitEnvironment()返回的元组,对应了(mainclass args, jars classpath, sparkConf, mainclass path)
    (childArgs, childClasspath, sparkConf, childMainClass)
  }

  // [SPARK-20328]. HadoopRDD calls into a Hadoop library that fetches delegation tokens with
  // renewer set to the YARN ResourceManager.  Since YARN isn't configured in Mesos mode, we
  // must trick it into thinking we're YARN.
  private def setRMPrincipal(sparkConf: SparkConf): Unit = {
    val shortUserName = UserGroupInformation.getCurrentUser.getShortUserName
    val key = s"spark.hadoop.${YarnConfiguration.RM_PRINCIPAL}"
    // scalastyle:off println
    printStream.println(s"Setting ${key} to ${shortUserName}")
    // scalastyle:off println
    sparkConf.set(key, shortUserName)
  }

  /**
   * 使用提供的启动环境运行子类的main方法。
   * 请注意，如果我们正在运行集群部署模式或python应用程序，则该主类将不是用户提供的主类。
   *
   * 这里的参数有子类需要的参数,子类路径,sparkConf,子类main()路径,参数重复判断
   */
  private def runMain(
                       childArgs: Seq[String],
                       childClasspath: Seq[String],
                       sparkConf: SparkConf,
                       childMainClass: String,
                       verbose: Boolean): Unit = {
    if (verbose) {
      printStream.println(s"Main class:\n$childMainClass")
      printStream.println(s"Arguments:\n${childArgs.mkString("\n")}")
      printStream.println(s"Spark config:\n${Utils.redact(sparkConf.getAll.toMap).mkString("\n")}")
      printStream.println(s"Classpath elements:\n${childClasspath.mkString("\n")}")
      printStream.println("\n")
    }

    // 初始化类加载器
    val loader = if (sparkConf.get(DRIVER_USER_CLASS_PATH_FIRST)) {
      // 如果用户设置了class,通过ChildFirstURLClassLoader来加载
      new ChildFirstURLClassLoader(new Array[URL](0), Thread.currentThread.getContextClassLoader)
    } else {
      // 如果用户没有设置,通过MutableURLClassLoader来加载
      new MutableURLClassLoader(new Array[URL](0), Thread.currentThread.getContextClassLoader)
    }
    // 设置由上面自定义的类加载器来加载class到JVM
    Thread.currentThread.setContextClassLoader(loader)

    // 从Classpath中添加jars
    for (jar <- childClasspath) {
      addJarToClasspath(jar, loader)
    }

    var mainClass: Class[_] = null
    try {
      // 反射加载子类mainclass
      mainClass = Utils.classForName(childMainClass)
    } catch {
      // 如果没找到类,并且类中包含hive的thriftserver,提示需要启动hive服务
      case e: ClassNotFoundException =>
        e.printStackTrace(printStream)
        if (childMainClass.contains("thriftserver")) {
          printStream.println(s"Failed to load main class $childMainClass.")
          printStream.println("You need to build Spark with -Phive and -Phive-thriftserver.")
        }
        System.exit(CLASS_NOT_FOUND_EXIT_STATUS)
      case e: NoClassDefFoundError =>
        e.printStackTrace(printStream)
        if (e.getMessage.contains("org/apache/hadoop/hive")) {
          printStream.println(s"Failed to load hive class.")
          printStream.println("You need to build Spark with -Phive and -Phive-thriftserver.")
        }
        System.exit(CLASS_NOT_FOUND_EXIT_STATUS)
    }


    /**
     * 通过classOf[]构建从属于mainClass的SparkApplication对象
     * 然后通过mainclass实例化了SparkApplication
     * SparkApplication是一个抽象类,这里主要是实现它的start()
     */
    val app: SparkApplication = if (classOf[SparkApplication].isAssignableFrom(mainClass)) {
      mainClass.newInstance().asInstanceOf[SparkApplication]
    } else {
      // SPARK-4170
      if (classOf[scala.App].isAssignableFrom(mainClass)) {
        printWarning("Subclasses of scala.App may not work correctly. Use a main() method instead.")
      }
      // 如果mainclass无法实例化SparkApplication,则使用替代构建子类JavaMainApplication实例
      new JavaMainApplication(mainClass)
    }

    @tailrec
    def findCause(t: Throwable): Throwable = t match {
      case e: UndeclaredThrowableException =>
        if (e.getCause() != null) findCause(e.getCause()) else e
      case e: InvocationTargetException =>
        if (e.getCause() != null) findCause(e.getCause()) else e
      case e: Throwable =>
        e
    }

    try {
      // 启动实例
      app.start(childArgs.toArray, sparkConf)
    } catch {
      case t: Throwable =>
        findCause(t) match {
          case SparkUserAppException(exitCode) =>
            System.exit(exitCode)

          case t: Throwable =>
            throw t
        }
    }
  }

  private[deploy] def addJarToClasspath(localJar: String, loader: MutableURLClassLoader) {
    val uri = Utils.resolveURI(localJar)
    uri.getScheme match {
      case "file" | "local" =>
        val file = new File(uri.getPath)
        if (file.exists()) {
          loader.addURL(file.toURI.toURL)
        } else {
          printWarning(s"Local jar $file does not exist, skipping.")
        }
      case _ =>
        printWarning(s"Skip remote jar $uri.")
    }
  }

  /**
   * Return whether the given primary resource represents a user jar.
   */
  private[deploy] def isUserJar(res: String): Boolean = {
    !isShell(res) && !isPython(res) && !isInternal(res) && !isR(res)
  }

  /**
   * Return whether the given primary resource represents a shell.
   */
  private[deploy] def isShell(res: String): Boolean = {
    (res == SPARK_SHELL || res == PYSPARK_SHELL || res == SPARKR_SHELL)
  }

  /**
   * Return whether the given main class represents a sql shell.
   */
  private[deploy] def isSqlShell(mainClass: String): Boolean = {
    mainClass == "org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver"
  }

  /**
   * Return whether the given main class represents a thrift server.
   */
  private def isThriftServer(mainClass: String): Boolean = {
    mainClass == "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2"
  }

  /**
   * Return whether the given primary resource requires running python.
   */
  private[deploy] def isPython(res: String): Boolean = {
    res != null && res.endsWith(".py") || res == PYSPARK_SHELL
  }

  /**
   * Return whether the given primary resource requires running R.
   */
  private[deploy] def isR(res: String): Boolean = {
    res != null && res.endsWith(".R") || res == SPARKR_SHELL
  }

  private[deploy] def isInternal(res: String): Boolean = {
    res == SparkLauncher.NO_RESOURCE
  }

  /**
   * Merge a sequence of comma-separated file lists, some of which may be null to indicate
   * no files, into a single comma-separated string.
   */
  private[deploy] def mergeFileLists(lists: String*): String = {
    val merged = lists.filterNot(StringUtils.isBlank)
      .flatMap(_.split(","))
      .mkString(",")
    if (merged == "") null else merged
  }

}

/** Provides utility functions to be used inside SparkSubmit. */
private[spark] object SparkSubmitUtils {

  // Exposed for testing
  var printStream = SparkSubmit.printStream

  // Exposed for testing.
  // These components are used to make the default exclusion rules for Spark dependencies.
  // We need to specify each component explicitly, otherwise we miss spark-streaming-kafka-0-8 and
  // other spark-streaming utility components. Underscore is there to differentiate between
  // spark-streaming_2.1x and spark-streaming-kafka-0-8-assembly_2.1x
  val IVY_DEFAULT_EXCLUDES = Seq("catalyst_", "core_", "graphx_", "kvstore_", "launcher_", "mllib_",
    "mllib-local_", "network-common_", "network-shuffle_", "repl_", "sketch_", "sql_", "streaming_",
    "tags_", "unsafe_")

  /**
   * Represents a Maven Coordinate
   *
   * @param groupId    the groupId of the coordinate
   * @param artifactId the artifactId of the coordinate
   * @param version    the version of the coordinate
   */
  private[deploy] case class MavenCoordinate(groupId: String, artifactId: String, version: String) {
    override def toString: String = s"$groupId:$artifactId:$version"
  }

  /**
   * Extracts maven coordinates from a comma-delimited string. Coordinates should be provided
   * in the format `groupId:artifactId:version` or `groupId/artifactId:version`.
   *
   * @param coordinates Comma-delimited string of maven coordinates
   * @return Sequence of Maven coordinates
   */
  def extractMavenCoordinates(coordinates: String): Seq[MavenCoordinate] = {
    coordinates.split(",").map { p =>
      val splits = p.replace("/", ":").split(":")
      require(splits.length == 3, s"Provided Maven Coordinates must be in the form " +
        s"'groupId:artifactId:version'. The coordinate provided is: $p")
      require(splits(0) != null && splits(0).trim.nonEmpty, s"The groupId cannot be null or " +
        s"be whitespace. The groupId provided is: ${splits(0)}")
      require(splits(1) != null && splits(1).trim.nonEmpty, s"The artifactId cannot be null or " +
        s"be whitespace. The artifactId provided is: ${splits(1)}")
      require(splits(2) != null && splits(2).trim.nonEmpty, s"The version cannot be null or " +
        s"be whitespace. The version provided is: ${splits(2)}")
      new MavenCoordinate(splits(0), splits(1), splits(2))
    }
  }

  /** Path of the local Maven cache. */
  private[spark] def m2Path: File = {
    if (Utils.isTesting) {
      // test builds delete the maven cache, and this can cause flakiness
      new File("dummy", ".m2" + File.separator + "repository")
    } else {
      new File(System.getProperty("user.home"), ".m2" + File.separator + "repository")
    }
  }

  /**
   * Extracts maven coordinates from a comma-delimited string
   *
   * @param defaultIvyUserDir The default user path for Ivy
   * @return A ChainResolver used by Ivy to search for and resolve dependencies.
   */
  def createRepoResolvers(defaultIvyUserDir: File): ChainResolver = {
    // We need a chain resolver if we want to check multiple repositories
    val cr = new ChainResolver
    cr.setName("spark-list")

    val localM2 = new IBiblioResolver
    localM2.setM2compatible(true)
    localM2.setRoot(m2Path.toURI.toString)
    localM2.setUsepoms(true)
    localM2.setName("local-m2-cache")
    cr.add(localM2)

    val localIvy = new FileSystemResolver
    val localIvyRoot = new File(defaultIvyUserDir, "local")
    localIvy.setLocal(true)
    localIvy.setRepository(new FileRepository(localIvyRoot))
    val ivyPattern = Seq(localIvyRoot.getAbsolutePath, "[organisation]", "[module]", "[revision]",
      "ivys", "ivy.xml").mkString(File.separator)
    localIvy.addIvyPattern(ivyPattern)
    val artifactPattern = Seq(localIvyRoot.getAbsolutePath, "[organisation]", "[module]",
      "[revision]", "[type]s", "[artifact](-[classifier]).[ext]").mkString(File.separator)
    localIvy.addArtifactPattern(artifactPattern)
    localIvy.setName("local-ivy-cache")
    cr.add(localIvy)

    // the biblio resolver resolves POM declared dependencies
    val br: IBiblioResolver = new IBiblioResolver
    br.setM2compatible(true)
    br.setUsepoms(true)
    br.setName("central")
    cr.add(br)

    val sp: IBiblioResolver = new IBiblioResolver
    sp.setM2compatible(true)
    sp.setUsepoms(true)
    sp.setRoot("http://dl.bintray.com/spark-packages/maven")
    sp.setName("spark-packages")
    cr.add(sp)
    cr
  }

  /**
   * Output a comma-delimited list of paths for the downloaded jars to be added to the classpath
   * (will append to jars in SparkSubmit).
   *
   * @param artifacts      Sequence of dependencies that were resolved and retrieved
   * @param cacheDirectory directory where jars are cached
   * @return a comma-delimited list of paths for the dependencies
   */
  def resolveDependencyPaths(
                              artifacts: Array[AnyRef],
                              cacheDirectory: File): String = {
    artifacts.map { artifactInfo =>
      val artifact = artifactInfo.asInstanceOf[Artifact].getModuleRevisionId
      cacheDirectory.getAbsolutePath + File.separator +
        s"${artifact.getOrganisation}_${artifact.getName}-${artifact.getRevision}.jar"
    }.mkString(",")
  }

  /** Adds the given maven coordinates to Ivy's module descriptor. */
  def addDependenciesToIvy(
                            md: DefaultModuleDescriptor,
                            artifacts: Seq[MavenCoordinate],
                            ivyConfName: String): Unit = {
    artifacts.foreach { mvn =>
      val ri = ModuleRevisionId.newInstance(mvn.groupId, mvn.artifactId, mvn.version)
      val dd = new DefaultDependencyDescriptor(ri, false, false)
      dd.addDependencyConfiguration(ivyConfName, ivyConfName + "(runtime)")
      // scalastyle:off println
      printStream.println(s"${dd.getDependencyId} added as a dependency")
      // scalastyle:on println
      md.addDependency(dd)
    }
  }

  /** Add exclusion rules for dependencies already included in the spark-assembly */
  def addExclusionRules(
                         ivySettings: IvySettings,
                         ivyConfName: String,
                         md: DefaultModuleDescriptor): Unit = {
    // Add scala exclusion rule
    md.addExcludeRule(createExclusion("*:scala-library:*", ivySettings, ivyConfName))

    IVY_DEFAULT_EXCLUDES.foreach { comp =>
      md.addExcludeRule(createExclusion(s"org.apache.spark:spark-$comp*:*", ivySettings,
        ivyConfName))
    }
  }

  /**
   * Build Ivy Settings using options with default resolvers
   *
   * @param remoteRepos Comma-delimited string of remote repositories other than maven central
   * @param ivyPath     The path to the local ivy repository
   * @return An IvySettings object
   */
  def buildIvySettings(remoteRepos: Option[String], ivyPath: Option[String]): IvySettings = {
    val ivySettings: IvySettings = new IvySettings
    processIvyPathArg(ivySettings, ivyPath)

    // create a pattern matcher
    ivySettings.addMatcher(new GlobPatternMatcher)
    // create the dependency resolvers
    val repoResolver = createRepoResolvers(ivySettings.getDefaultIvyUserDir)
    ivySettings.addResolver(repoResolver)
    ivySettings.setDefaultResolver(repoResolver.getName)
    processRemoteRepoArg(ivySettings, remoteRepos)
    ivySettings
  }

  /**
   * Load Ivy settings from a given filename, using supplied resolvers
   *
   * @param settingsFile Path to Ivy settings file
   * @param remoteRepos  Comma-delimited string of remote repositories other than maven central
   * @param ivyPath      The path to the local ivy repository
   * @return An IvySettings object
   */
  def loadIvySettings(
                       settingsFile: String,
                       remoteRepos: Option[String],
                       ivyPath: Option[String]): IvySettings = {
    val file = new File(settingsFile)
    require(file.exists(), s"Ivy settings file $file does not exist")
    require(file.isFile(), s"Ivy settings file $file is not a normal file")
    val ivySettings: IvySettings = new IvySettings
    try {
      ivySettings.load(file)
    } catch {
      case e@(_: IOException | _: ParseException) =>
        throw new SparkException(s"Failed when loading Ivy settings from $settingsFile", e)
    }
    processIvyPathArg(ivySettings, ivyPath)
    processRemoteRepoArg(ivySettings, remoteRepos)
    ivySettings
  }

  /* Set ivy settings for location of cache, if option is supplied */
  private def processIvyPathArg(ivySettings: IvySettings, ivyPath: Option[String]): Unit = {
    ivyPath.filterNot(_.trim.isEmpty).foreach { alternateIvyDir =>
      ivySettings.setDefaultIvyUserDir(new File(alternateIvyDir))
      ivySettings.setDefaultCache(new File(alternateIvyDir, "cache"))
    }
  }

  /* Add any optional additional remote repositories */
  private def processRemoteRepoArg(ivySettings: IvySettings, remoteRepos: Option[String]): Unit = {
    remoteRepos.filterNot(_.trim.isEmpty).map(_.split(",")).foreach { repositoryList =>
      val cr = new ChainResolver
      cr.setName("user-list")

      // add current default resolver, if any
      Option(ivySettings.getDefaultResolver).foreach(cr.add)

      // add additional repositories, last resolution in chain takes precedence
      repositoryList.zipWithIndex.foreach { case (repo, i) =>
        val brr: IBiblioResolver = new IBiblioResolver
        brr.setM2compatible(true)
        brr.setUsepoms(true)
        brr.setRoot(repo)
        brr.setName(s"repo-${i + 1}")
        cr.add(brr)
        // scalastyle:off println
        printStream.println(s"$repo added as a remote repository with the name: ${brr.getName}")
        // scalastyle:on println
      }

      ivySettings.addResolver(cr)
      ivySettings.setDefaultResolver(cr.getName)
    }
  }

  /** A nice function to use in tests as well. Values are dummy strings. */
  def getModuleDescriptor: DefaultModuleDescriptor = DefaultModuleDescriptor.newDefaultInstance(
    // Include UUID in module name, so multiple clients resolving maven coordinate at the same time
    // do not modify the same resolution file concurrently.
    ModuleRevisionId.newInstance("org.apache.spark",
      s"spark-submit-parent-${UUID.randomUUID.toString}",
      "1.0"))

  /**
   * Clear ivy resolution from current launch. The resolution file is usually at
   * ~/.ivy2/org.apache.spark-spark-submit-parent-$UUID-default.xml,
   * ~/.ivy2/resolved-org.apache.spark-spark-submit-parent-$UUID-1.0.xml, and
   * ~/.ivy2/resolved-org.apache.spark-spark-submit-parent-$UUID-1.0.properties.
   * Since each launch will have its own resolution files created, delete them after
   * each resolution to prevent accumulation of these files in the ivy cache dir.
   */
  private def clearIvyResolutionFiles(
                                       mdId: ModuleRevisionId,
                                       ivySettings: IvySettings,
                                       ivyConfName: String): Unit = {
    val currentResolutionFiles = Seq(
      s"${mdId.getOrganisation}-${mdId.getName}-$ivyConfName.xml",
      s"resolved-${mdId.getOrganisation}-${mdId.getName}-${mdId.getRevision}.xml",
      s"resolved-${mdId.getOrganisation}-${mdId.getName}-${mdId.getRevision}.properties"
    )
    currentResolutionFiles.foreach { filename =>
      new File(ivySettings.getDefaultCache, filename).delete()
    }
  }

  /**
   * Resolves any dependencies that were supplied through maven coordinates
   *
   * @param coordinates Comma-delimited string of maven coordinates
   * @param ivySettings An IvySettings containing resolvers to use
   * @param exclusions  Exclusions to apply when resolving transitive dependencies
   * @return The comma-delimited path to the jars of the given maven artifacts including their
   *         transitive dependencies
   */
  def resolveMavenCoordinates(
                               coordinates: String,
                               ivySettings: IvySettings,
                               exclusions: Seq[String] = Nil,
                               isTest: Boolean = false): String = {
    if (coordinates == null || coordinates.trim.isEmpty) {
      ""
    } else {
      val sysOut = System.out
      try {
        // To prevent ivy from logging to system out
        System.setOut(printStream)
        val artifacts = extractMavenCoordinates(coordinates)
        // Directories for caching downloads through ivy and storing the jars when maven coordinates
        // are supplied to spark-submit
        val packagesDirectory: File = new File(ivySettings.getDefaultIvyUserDir, "jars")
        // scalastyle:off println
        printStream.println(
          s"Ivy Default Cache set to: ${ivySettings.getDefaultCache.getAbsolutePath}")
        printStream.println(s"The jars for the packages stored in: $packagesDirectory")
        // scalastyle:on println

        val ivy = Ivy.newInstance(ivySettings)
        // Set resolve options to download transitive dependencies as well
        val resolveOptions = new ResolveOptions
        resolveOptions.setTransitive(true)
        val retrieveOptions = new RetrieveOptions
        // Turn downloading and logging off for testing
        if (isTest) {
          resolveOptions.setDownload(false)
          resolveOptions.setLog(LogOptions.LOG_QUIET)
          retrieveOptions.setLog(LogOptions.LOG_QUIET)
        } else {
          resolveOptions.setDownload(true)
        }

        // Default configuration name for ivy
        val ivyConfName = "default"

        // A Module descriptor must be specified. Entries are dummy strings
        val md = getModuleDescriptor

        md.setDefaultConf(ivyConfName)

        // Add exclusion rules for Spark and Scala Library
        addExclusionRules(ivySettings, ivyConfName, md)
        // add all supplied maven artifacts as dependencies
        addDependenciesToIvy(md, artifacts, ivyConfName)
        exclusions.foreach { e =>
          md.addExcludeRule(createExclusion(e + ":*", ivySettings, ivyConfName))
        }
        // resolve dependencies
        val rr: ResolveReport = ivy.resolve(md, resolveOptions)
        if (rr.hasError) {
          throw new RuntimeException(rr.getAllProblemMessages.toString)
        }
        // retrieve all resolved dependencies
        ivy.retrieve(rr.getModuleDescriptor.getModuleRevisionId,
          packagesDirectory.getAbsolutePath + File.separator +
            "[organization]_[artifact]-[revision](-[classifier]).[ext]",
          retrieveOptions.setConfs(Array(ivyConfName)))
        val paths = resolveDependencyPaths(rr.getArtifacts.toArray, packagesDirectory)
        val mdId = md.getModuleRevisionId
        clearIvyResolutionFiles(mdId, ivySettings, ivyConfName)
        paths
      } finally {
        System.setOut(sysOut)
      }
    }
  }

  private[deploy] def createExclusion(
                                       coords: String,
                                       ivySettings: IvySettings,
                                       ivyConfName: String): ExcludeRule = {
    val c = extractMavenCoordinates(coords)(0)
    val id = new ArtifactId(new ModuleId(c.groupId, c.artifactId), "*", "*", "*")
    val rule = new DefaultExcludeRule(id, ivySettings.getMatcher("glob"), null)
    rule.addConfiguration(ivyConfName)
    rule
  }

}

/**
 * Provides an indirection layer for passing arguments as system properties or flags to
 * the user's driver program or to downstream launcher tools.
 */
private case class OptionAssigner(
                                   value: String,
                                   clusterManager: Int,
                                   deployMode: Int,
                                   clOption: String = null,
                                   confKey: String = null)
