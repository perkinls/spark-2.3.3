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

package org.apache.spark.launcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.launcher.CommandBuilderUtils.*;

/**
 * Command line interface for the Spark launcher. Used internally by Spark scripts.
 * 这是提供spark内部脚本使用工具类
 */
class Main {

    /**
     * Usage: Main [class] [class args]
     * <p>
     * 分为spark-submit和spark-class两种模式
     * 如果提交的是class类的话,会包含其他如:master/worker/history等等
     * This CLI works in two different modes:
     * <ul>
     *   <li>"spark-submit": if <i>class</i> is "org.apache.spark.deploy.SparkSubmit", the
     *   {@link SparkLauncher} class is used to launch a Spark application.</li>
     *   <li>"spark-class": if another class is provided, an internal Spark class is run.</li>
     * </ul>
     * <p>
     * unix系统的输出的参数是集合,而windows参数是空格分隔
     * This class works in tandem with the "bin/spark-class" script on Unix-like systems, and
     * "bin/spark-class2.cmd" batch script on Windows to execute the final command.
     * <p>
     * On Unix-like systems, the output is a list of command arguments, separated by the NULL
     * character. On Windows, the output is a command line suitable for direct execution from the
     * script.
     * <p>
     * spark-class提交过来的参数如下：
     * org.apache.spark.deploy.SparkSubmit \
     * --class com.lp.test.app.LocalPi \
     * --master local \
     * /Users/lipan/Desktop/spark-local/spark-local-train-1.0.jar
     */
    public static void main(String[] argsArray) throws Exception {
        checkArgument(argsArray.length > 0, "Not enough arguments: missing class name.");

        // 判断参数列表
        List<String> args = new ArrayList<>(Arrays.asList(argsArray));
        String className = args.remove(0);

        // 判断是否打印执行信息
        boolean printLaunchCommand = !isEmpty(System.getenv("SPARK_PRINT_LAUNCH_COMMAND"));
        // 创建命令解析器
        AbstractCommandBuilder builder;

        /**
         * 构建执行程序对象:spark-submit/spark-class
         * 把参数都取出并解析,放入执行程序对象中
         * 意思是,submit还是master和worker等程序在这里拆分,并获取对应的执行参数
         */
        if (className.equals("org.apache.spark.deploy.SparkSubmit")) {
            try {
                // 构建spark-submit命令对象
                builder = new SparkSubmitCommandBuilder(args);
            } catch (IllegalArgumentException e) {
                printLaunchCommand = false;
                System.err.println("Error: " + e.getMessage());
                System.err.println();

                // 类名解析--class org.apache.spark.repl.Main
                MainClassOptionParser parser = new MainClassOptionParser();
                try {
                    parser.parse(args);
                } catch (Exception ignored) {
                    // Ignore parsing exceptions.
                }
                // 帮助信息
                List<String> help = new ArrayList<>();
                if (parser.className != null) {
                    help.add(parser.CLASS);
                    help.add(parser.className);
                }
                help.add(parser.USAGE_ERROR);
                // 构建spark-submit帮助信息对象
                builder = new SparkSubmitCommandBuilder(help);
            }
        } else {
            // 构建spark-class命令对象
            // 主要是在这个类里解析了命令对象和参数
            builder = new SparkClassCommandBuilder(className, args);
        }

        /**
         * 这里才真正构建了执行命令
         * 调用了SparkClassCommandBuilder的buildCommand方法
         * 把执行参数解析成了k/v格式
         */
        Map<String, String> env = new HashMap<>();
        List<String> cmd = builder.buildCommand(env);
        if (printLaunchCommand) {
            System.err.println("Spark Command: " + join(" ", cmd));
            System.err.println("========================================");
        }

        if (isWindows()) {
            System.out.println(prepareWindowsCommand(cmd, env));
        } else {
            // In bash, use NULL as the arg separator since it cannot be used in an argument.

            /**
             * 输出参数：/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/bin/java
             * -cp /Users/lipan/workspace/source_code/spark-2.3.3/conf/:/Users/lipan/workspace/source_code/spark-2.3.3/assembly/target/scala-2.11/jars/*
             * -Xmx1g org.apache.spark.deploy.SparkSubmit
             * --master local
             * --class com.lp.test.app.LocalPi
             * /Users/lipan/Desktop/spark-local/original-spark-local-train-1.0.jar 10
             *  java -cp / org.apache.spark.deploy.SparkSubmit启动该类
             */
            List<String> bashCmd = prepareBashCommand(cmd, env);
            for (String c : bashCmd) {
                System.out.print(c);
                System.out.print('\0');
            }
        }
    }

    /**
     * Prepare a command line for execution from a Windows batch script.
     * <p>
     * The method quotes all arguments so that spaces are handled as expected. Quotes within arguments
     * are "double quoted" (which is batch for escaping a quote). This page has more details about
     * quoting and other batch script fun stuff: http://ss64.com/nt/syntax-esc.html
     */
    private static String prepareWindowsCommand(List<String> cmd, Map<String, String> childEnv) {
        StringBuilder cmdline = new StringBuilder();
        for (Map.Entry<String, String> e : childEnv.entrySet()) {
            cmdline.append(String.format("set %s=%s", e.getKey(), e.getValue()));
            cmdline.append(" && ");
        }
        for (String arg : cmd) {
            cmdline.append(quoteForBatchScript(arg));
            cmdline.append(" ");
        }
        return cmdline.toString();
    }

    /**
     * Prepare the command for execution from a bash script. The final command will have commands to
     * set up any needed environment variables needed by the child process.
     */
    private static List<String> prepareBashCommand(List<String> cmd, Map<String, String> childEnv) {
        if (childEnv.isEmpty()) {
            return cmd;
        }

        List<String> newCmd = new ArrayList<>();
        newCmd.add("env");

        for (Map.Entry<String, String> e : childEnv.entrySet()) {
            newCmd.add(String.format("%s=%s", e.getKey(), e.getValue()));
        }
        newCmd.addAll(cmd);
        return newCmd;
    }

    /**
     * 当spark-submit提交失败时,这里会再进行一次解析,再不行才会提示用法
     */
    private static class MainClassOptionParser extends SparkSubmitOptionParser {

        String className;

        @Override
        protected boolean handle(String opt, String value) {
            if (CLASS.equals(opt)) {
                className = value;
            }
            return false;
        }

        @Override
        protected boolean handleUnknown(String opt) {
            return false;
        }

        @Override
        protected void handleExtraArgs(List<String> extra) {

        }

    }

}
