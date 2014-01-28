/*
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
    "Mate1 Public Maven Repo" at "https://raw.github.com/mate1/maven/master/public/", // For Kafka, Camus
    "Cloudera Repo" at "https://repository.cloudera.com/artifactory/cloudera-repos/", // For Hadoop, ZK...
    "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/" // For Akka...
  )

  object V {
    // Versions
    val hadoop        = "2.0.0-mr1-cdh4.5.0"
    val hadoopHdfs    = "2.0.0-cdh4.5.0"
    val kafka         = "0.7.2-scala2.9.2"
    val camus         = "0.1.0-kafka0.7.2-scala2.9.2-SNAPSHOT"
    val avro          = "1.7.3"
    val akka          = "2.0.5"
    val zookeeper     = "3.4.5-cdh4.2.0"
  }

  object X {
    // Exclusions
    val avroOrg       = ExclusionRule(organization = "org.apache.avro")
    val hadoopOrg     = ExclusionRule(organization = "org.apache.hadoop")
    val commonsDaemon = ExclusionRule(organization = "commons-daemon", name = "commons-daemon")
    val camusApi      = ExclusionRule(organization = "com.linkedin.camus", name = "camus-api")
    val jacksonAsl    = ExclusionRule(organization = "org.codehaus.jackson", name = "jackson-mapper-asl")
    val zookeeper     = ExclusionRule(organization = "org.apache.zookeeper", name = "zookeeper")
  }

  object Libraries {
    val hadoopCore    = "org.apache.hadoop"          % "hadoop-core"         % V.hadoop        % "provided" excludeAll(X.avroOrg)
    val hadoopHdfs    = "org.apache.hadoop"          % "hadoop-hdfs"         % V.hadoopHdfs    % "provided" excludeAll(X.avroOrg, X.commonsDaemon)
    val kafkaCore     = "org.apache.kafka"           % "kafka"               % V.kafka excludeAll(X.avroOrg, X.hadoopOrg)
    val camusEtlKafka = "com.linkedin.camus"         % "camus-etl-kafka"     % V.camus excludeAll(X.avroOrg, X.camusApi, X.jacksonAsl, X.zookeeper)
    val camusApi      = "com.linkedin.camus"         % "camus-api"           % V.camus excludeAll(X.avroOrg)
    val avro          = "org.apache.avro"            % "avro"                % V.avro force()
    val avroMapRed    = "org.apache.avro"            % "avro-mapred"         % V.avro classifier "hadoop2"
    val akkaActor     = "com.typesafe.akka"          % "akka-actor"          % V.akka
    val akkaAgent     = "com.typesafe.akka"          % "akka-agent"          % V.akka
    val zookeeper     = "org.apache.zookeeper"       % "zookeeper"           % V.zookeeper
  }
}

