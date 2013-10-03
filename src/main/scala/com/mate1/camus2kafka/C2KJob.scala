package com.mate1.camus2kafka

import org.apache.hadoop.mapreduce.Job
import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import scala.annotation.tailrec
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import scala.io.Source
import scala.collection.JavaConverters._

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
  val HDFS_INPUT_PATH = PREFIX+"hdfs.input.path"
  val AVRO_OUTPUT_SCHEMA = PREFIX+"avro.output.schema"
  val AVRO_OUTPUT_SCHEMA_PATH = PREFIX+"avro.output.schema.path"
  val KAFKA_REPLAY_TOPIC = PREFIX+"kafka.replay.topic"
  val KAFKA_SOURCE_TOPIC = PREFIX+"kafka.source.topic"
  val KAFKA_TOPIC = PREFIX+"kafka.topic"
  val KAFKA_CONSUMER_GROUP = PREFIX+"kafka.consumer.group"
  val ZK_HOSTS = PREFIX+"zk.hosts"
  val CAMUS_DEST_DIR = PREFIX+"camus.dest.dir"
  val CAMUS_EXEC_DIR = PREFIX+"camus.exec.dir"
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

  // Schema to be used to encode the messages we send to Kafka
  lazy val outputSchema = new Schema.Parser().parse(config.get(AVRO_OUTPUT_SCHEMA))


  // The Kafka topic where Camus2Kafka publishes all of the currently ingested records
  lazy val replayTopic = config.get(KAFKA_REPLAY_TOPIC)

  // The original Kafka topic Camus read from
  lazy val sourceTopic = config.get(KAFKA_SOURCE_TOPIC)

  // The config object that is used to get the lazy vals above
  var config : Configuration = null
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
    if (validateConfig(conf)) {
      config = conf
      setSchema()
    } else {
      false
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
        println("Ex: package.JobClassName -D %s=value\n".format(params.head))
        false
      }
    }
  }

  private def setSchema() : Boolean = try {
    val outputSchemaFile = new Path(config.get(AVRO_OUTPUT_SCHEMA_PATH))
    val fs : FileSystem = outputSchemaFile.getFileSystem(config)
    val inputStream : FSDataInputStream = fs.open(outputSchemaFile)
    val SCHEMA = Source.fromInputStream(inputStream).mkString
    inputStream.close()
    config.setStrings(AVRO_OUTPUT_SCHEMA, SCHEMA)
    true
  } catch {
    case e => {
      println("Error: Can't set the output schema.")
      e.printStackTrace()
      false
    }
  }
}

trait C2KJob extends C2KJobConfig {

  /**
   * Tries to init the config before running the job. If the config fails to init then the job won't run
   * @param job The job to run
   * @return true if the job was successful, false otherwise
   */
  def runJob(job : Job) : Boolean = {
    initConfig(job.getConfiguration) match {
      case true => job.waitForCompletion(true)

      case _ => {
        println("Error setting the Camus2Kafka configuration, please check your configuration")
        false
      }
    }
  }

  /**
   * Success Callback that gets called by the JobRunner
   */
  def successCallback = println("Everything was OK!")

  /**
   * Error Callback that gets called by the JobRunner
   */
  def errorCallback = println("Something went wrong!")

}