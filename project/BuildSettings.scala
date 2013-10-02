/*
 * Copyright (c) 2013 Mate1 Inc. All rights reserved.
 * Copyright (c) 2012 SnowPlow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
import sbt._
import Keys._

object BuildSettings {

  // Basic settings for our app
  lazy val basicSettings = Seq[Setting[_]](
    organization  := "Mate1 Inc.",
    version       := "0.0.1",
    description   := "Moves data stored in Hadoop through Camus into Kafka.",
    scalaVersion  := "2.9.2", // -> 2.10.0 when Scalding is ready
    scalacOptions := Seq("-deprecation", "-encoding", "utf8"),
    resolvers     ++= Dependencies.resolutionRepos
  )

  // sbt-assembly settings for building a fat jar
  import sbtassembly.Plugin._
  import AssemblyKeys._
  lazy val sbtAssemblySettings = assemblySettings ++ Seq(

    // Slightly cleaner jar name
    jarName in assembly <<= (name, version) { (name, version) => name + "-" + version + ".jar" },
    
    // Drop these jars
    excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
      val excludes = Set(
        "jsp-api-2.1-6.1.14.jar",
        "jsp-2.1-6.1.14.jar",
        "jasper-compiler-5.5.12.jar",
        "minlog-1.2.jar", // Otherwise causes conflicts with Kyro (which bundles it)
        "janino-2.5.16.jar", // Janino includes a broken signature, and is not needed anyway
        "commons-beanutils-core-1.8.0.jar", // Clash with each other and with commons-collections
        "commons-beanutils-1.7.0.jar",
        "avro-1.3.2.jar",
        "avro-1.6.1.jar",
        "servlet-api-2.5-6.1.14.jar",
        "servlet-api-2.5-20081211.jar",
        "jsp-api-2.0.jar",
        "asm-3.2.jar",
        "netty-3.2.6.Final.jar",
        "netty-3.2.2.Final.jar"
      ) 
      cp filter { jar => excludes(jar.data.getName) }
    },
    
    mergeStrategy in assembly <<= (mergeStrategy in assembly) {
      (old) => {
        case old if old.startsWith("org/apache/commons/") => MergeStrategy.first
        case old if old.startsWith("org/hamcrest/") => MergeStrategy.first
        case old if old.startsWith("META-INF/maven/") => MergeStrategy.first
        case old if old.endsWith("package-info.class") => MergeStrategy.first
        case x => old(x)
      }
    }
  )

  lazy val buildSettings = basicSettings ++ sbtAssemblySettings
}
