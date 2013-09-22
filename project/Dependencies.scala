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

object Dependencies {
  val resolutionRepos = Seq(
    ScalaToolsSnapshots,
    "Concurrent Maven Repo" at "http://conjars.org/repo", // For Scalding, Cascading etc
    "Mate1 Maven Repo" at "http://maven.mate1:8081/nexus/content/groups/public" // For Kafka, Camus
  )

  object V {
    val scalding  = "0.8.5"
    val hadoop    = "0.20.2"
    val specs2    = "1.12.3" // -> "1.13" when we bump to Scala 2.10.0
    val kafka     = "0.7.2"
    val camus     = "0.1.0-SNAPSHOT" 
  }

  object Libraries {
    val scaldingCore  = "com.twitter"                %%  "scalding-core"       % V.scalding
    val hadoopCore    = "org.apache.hadoop"          %   "hadoop-core"         % V.hadoop       % "provided"
    val kafkaCore     = "kafka"                      %%  "kafka"               % V.kafka
    val camusEtlKafka = "com.linkedin.camus"         %   "camus-etl-kafka"     % V.camus
    val camusApi      = "com.linkedin.camus"         %   "camus-api"           % V.camus

    // Scala (test only)
    val specs2       = "org.specs2"                 %% "specs2"               % V.specs2       % "test"
  }
}

