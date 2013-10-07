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
    "Mate1 Maven Repo" at "http://maven.mate1:8081/nexus/content/groups/public", // For Kafka, Camus
    ScalaToolsSnapshots,
    "Concurrent Maven Repo" at "http://conjars.org/repo", // For Scalding, Cascading etc
    "Cloudera Repo" at "https://repository.cloudera.com/artifactory/cloudera-repos/", // For Hadoop stuff
    "Radlab Repository" at "http://scads.knowsql.org/nexus/content/groups/public/" // edu.berkeley.cs
  )

  object V {
    val hadoop        = "2.0.0-mr1-cdh4.2.0"
    val hadoopHdfs    = "2.0.0-cdh4.2.0"
    val specs2        = "1.12.3" // -> "1.13" when we bump to Scala 2.10.0
    val kafka         = "0.7.2"
    val camus         = "0.1.0-kafka0.7.2-scala2.9.2-SNAPSHOT"
    val avro          = "1.7.3"
    val akka          = "2.0.5"
  }

  object Libraries {
    val hadoopCore    = "org.apache.hadoop"          %   "hadoop-core"         % V.hadoop        % "provided" excludeAll(ExclusionRule(organization = "org.apache.avro")) 
    val hadoopHdfs    = "org.apache.hadoop"          %   "hadoop-hdfs"         % V.hadoopHdfs    % "provided" excludeAll(ExclusionRule(organization = "org.apache.avro"), ExclusionRule(organization = "commons-daemon", name = "commons-daemon"))
    val kafkaCore     = "kafka"                      %%  "kafka"               % V.kafka excludeAll(ExclusionRule(organization = "org.apache.avro"), ExclusionRule(organization = "org.apache.hadoop"))
    val camusEtlKafka = "com.linkedin.camus"         %   "camus-etl-kafka"     % V.camus excludeAll(ExclusionRule(organization = "org.apache.avro"))
    val camusApi      = "com.linkedin.camus"         %   "camus-api"           % V.camus excludeAll(ExclusionRule(organization = "org.apache.avro"))
    val avro         = "org.apache.avro"             %  "avro"                % V.avro  force()
    val avroMapRed         = "org.apache.avro"             %  "avro-mapred"                % V.avro classifier "hadoop2"
    val akkaActor     = "com.typesafe.akka"          %  "akka-actor"          % V.akka
    val akkaAgent     = "com.typesafe.akka"          %  "akka-agent"          % V.akka

    // Scala (test only)
    val specs2       = "org.specs2"                 %% "specs2"               % V.specs2       % "test"
  }
}

