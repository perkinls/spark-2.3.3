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

import java.lang.reflect.Modifier

import org.apache.spark.SparkConf

/**
 * 这是spark任务的入口抽象类,需要实现它的无参构造
 */
private[spark] trait SparkApplication {

  def start(args: Array[String], conf: SparkConf): Unit

}

/**
 * 用main方法包装标准java类的SparkApplication实现
 *
 * 用main方法包装标准java类的SparkApplication实现配置是通过系统配置文件传递,在同一个JVM中加载太多配置会可能导致配置溢出
 */
private[deploy] class JavaMainApplication(klass: Class[_]) extends SparkApplication {

  override def start(args: Array[String], conf: SparkConf): Unit = {
    val mainMethod = klass.getMethod("main", new Array[String](0).getClass)
    if (!Modifier.isStatic(mainMethod.getModifiers)) {
      throw new IllegalStateException("The main method in the given main class must be static")
    }

    val sysProps = conf.getAll.toMap
    sysProps.foreach { case (k, v) =>
      sys.props(k) = v
    }

    mainMethod.invoke(null, args)
  }

}
