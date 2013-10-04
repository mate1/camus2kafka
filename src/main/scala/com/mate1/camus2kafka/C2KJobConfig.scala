package com.mate1.camus2kafka

import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import scala.annotation.tailrec
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import scala.io.Source
import scala.collection.JavaConverters._
import org.apache.hadoop.io.LongWritable

/**
 * Created with IntelliJ IDEA.
 * User: Boris Fersing
 * Date: 9/27/13
 * Time: 10:22 AM
 */


/**
 * This object contains the Camus2Kafka config that is used for a C2K job
 */
object C2KJobConfig {

  // String values for the config parameters we use
  val PREFIX = "c2k."
  val HDFS_INPUT_DIR = PREFIX+"hdfs.input.dir"
  val AVRO_OUTPUT_SCHEMA = PREFIX+"avro.output.schema"
  val AVRO_OUTPUT_SCHEMA_PATH = PREFIX+"avro.output.schema.path"
  val KAFKA_REPLAY_TOPIC = PREFIX+"kafka.replay.topic"
  val KAFKA_TOPIC = PREFIX+"kafka.topic"
  val KAFKA_CONSUMER_GROUP = PREFIX+"kafka.consumer.group"
  val ZK_HOSTS = PREFIX+"zk.hosts"
  val CAMUS_DEST_DIR = PREFIX+"camus.dest.dir"
  val CAMUS_EXEC_DIR = PREFIX+"camus.exec.dir"
  val CAMUS_HISTORY_DIR = PREFIX+"camus.history.dir"
  val REDUCER_CLASS = PREFIX+"reducer.class"
  val MAPPER_CLASS = PREFIX+"mapper.class"
  val MAPPER_OUTKEY_CLASS = PREFIX+"mapper.outkey.class"
  val PRINTCONF = PREFIX+"printconf"

  // Map of required parameters with their description
  val requiredParams = Map(
    CAMUS_DEST_DIR -> "The camus destination directory.",
    CAMUS_EXEC_DIR -> "The camus execution directory.",
    AVRO_OUTPUT_SCHEMA_PATH -> "The HDFS path of the avro schema to be used to encode the Kafka messages.",
    KAFKA_TOPIC -> "The Kafka topic you want to process.",
    KAFKA_CONSUMER_GROUP -> ("The (high-level) Kafka consumer group whose ZK offsets will be set by Camus2Kafka so that " +
      "the regular Kafka topic can be stitched back at the correct cut off point after re-consuming all of the replay topic."),
    ZK_HOSTS -> "The zookeeper hosts Camus2Kafka will connect to."
  )

  // The config object that is used to get the lazy vals below
  var config : Configuration = null


  // Schema to be used to encode the messages we send to Kafka
  lazy val outputSchema = {
    val outputSchemaFile = new Path(config.get(AVRO_OUTPUT_SCHEMA_PATH))
    val fs : FileSystem = outputSchemaFile.getFileSystem(config)
    val inputStream : FSDataInputStream = fs.open(outputSchemaFile)
    val SCHEMA = Source.fromInputStream(inputStream).mkString
    inputStream.close()
    new Schema.Parser().parse(SCHEMA)
  }

  // The original Kafka topic Camus read from
  lazy val sourceTopic = config.get(KAFKA_TOPIC)

  // The Camus destination dir that contains the data
  lazy val camusDestDir = config.get(CAMUS_DEST_DIR)

  // The Camus execution directory that contains the history and the offsets
  lazy val camusExecDir = config.get(CAMUS_EXEC_DIR)

  // The zookeeper hosts
  lazy val zkHosts = config.get(ZK_HOSTS)



  /**
   * The vals below are optional params that have a default value based on the required params.
   */

  // The HDFS input path that contains the avro data
  lazy val hdfsInputDir = config.get(HDFS_INPUT_DIR) match {
    case null => camusDestDir + "/" + sourceTopic
    case path => path
  }

  // The Kafka topic where Camus2Kafka publishes all of the currently ingested records
  lazy val replayTopic = config.get(KAFKA_REPLAY_TOPIC) match {
    case null => sourceTopic+"_REPLAY"
    case topic => topic
  }

  // The Camus history dir that contains the offsets
  lazy val camusHistoryDir = config.get(CAMUS_HISTORY_DIR) match {
    case null => camusExecDir + "/base/history"
    case dir => dir
  }
  
  // The mapper class (Default is Camus2KafkaMapperByTime)
  lazy val mapperClass  = config.get(MAPPER_CLASS) match {
    case null => classOf[Camus2KafkaMapperByTime].asInstanceOf[Class[AbstractC2KMapper[_]]]
    case className => Class.forName(className).asInstanceOf[Class[AbstractC2KMapper[_]]]
  }

  // The reducer class (Default is Camus2KafkaReducerByTime)
  lazy val reducerClass = config.get(REDUCER_CLASS) match {
    case null => classOf[Camus2KafkaReducerByTime].asInstanceOf[Class[AbstractC2KReducer[_]]]
    case className => Class.forName(className).asInstanceOf[Class[AbstractC2KReducer[_]]]
  }

  // The Mapper Out Key class (Default is LongWritable)
  lazy val mapperOutKeyClass = config.get(MAPPER_OUTKEY_CLASS) match {
    case null => classOf[LongWritable]
    case className => Class.forName(className)
  }

}


/**
 * The C2KJobConfig trait contains methods used to access and set values in the C2KJobConfig object
 */
trait C2KJobConfig {
  import C2KJobConfig._

  /**
   * Initialize the configuration. Must be called before running a C2K job
   * @param conf The config object
   * @return true if the config is valid, false otherwise
   */
  protected def initConfig(conf: Configuration) : Boolean = {
    config match {
      case null => {
        if (validateConfig(conf)) {
          config = conf

          // Make sure that the Schema is valid
          outputSchema != null

        } else {
          false
        }
      }
      case _ => true
    }
  }

  /**
   * Check if the config is valid and tell the user about the missing params
   * @param conf The config object
   * @return true if the config is valid, false otherwise
   */
  protected def validateConfig(conf: Configuration) : Boolean = {

    conf.get(PRINTCONF) match {
      case "custom" => conf.asScala.toList.filter(entry => entry.getKey.contains(PREFIX)).sortBy(entry => entry.getKey)
        .foreach(entry => println(entry.getKey+" : "+entry.getValue))
      case "ALL" => conf.asScala.toList.sortBy(entry => entry.getKey).foreach(entry => println(entry.getKey+" : "+entry.getValue))
      case _ => ()
    }

    @tailrec
    def getMissingParams(required: List[String], missing: List[String]) : List[String] = required match {
      case Nil => missing
      case arg::tail => if (conf.get(arg) == null) getMissingParams(tail, arg::missing) else getMissingParams(tail, missing)
    }

    getMissingParams(requiredParams.keys.toList, Nil) match {
      case Nil => {
        true
      }
      case params => {
        println("Missing parameters:\n")
        params.foreach(param => println("%s: %s".format(param, requiredParams.getOrElse(param, ""))))
        println()
        println("Please specify the parameters using the -D command line option.")
        println("Ex: hadoop jar camus2kafka.jar -D %s=value\n".format(params.head))
        false
      }
    }
  }
}