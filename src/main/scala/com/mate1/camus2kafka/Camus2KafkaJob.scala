package com.mate1.camus2kafka

import org.apache.hadoop.util.Tool
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat
import scala.Predef._
import org.apache.avro.mapred._
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.io.{BytesWritable, LongWritable, NullWritable}
import org.apache.avro.mapreduce.AvroKeyInputFormat

/**
 * Created with IntelliJ IDEA.
 * User: Boris Fersing
 * Date: 9/27/13
 * Time: 12:15 PM
 */

class Camus2KafkaJob extends Configured with Tool with C2KJobConfig{

  def run(args: Array[String]): Int = {
    val conf = getConf

    if (initConfig(conf)){
      val job = new Job(conf, "Camus to Kafka")

      // Settings based on the configuration provided by the user
      FileInputFormat.addInputPath(job, new Path(C2KJobConfig.hdfsInputDir))
      job.setJarByClass(C2KJobConfig.mapperClass)
      job.setMapperClass(C2KJobConfig.mapperClass)
      job.setReducerClass(C2KJobConfig.reducerClass)
      job.setMapOutputKeyClass(C2KJobConfig.mapperOutKeyClass)


      // Static settings
      job.setInputFormatClass(classOf[AvroKeyInputFormat[GenericRecord]])
      job.setMapOutputValueClass(classOf[BytesWritable])
      job.setOutputKeyClass(classOf[NullWritable])
      job.setOutputValueClass(classOf[NullWritable])
      job.setOutputFormatClass(classOf[NullOutputFormat[NullWritable, NullWritable]])

      job.setReduceSpeculativeExecution(false)

      // Do we want to skip the MapReduce task and only set the Zookeeper Offsets?
      if (C2KJobConfig.setZKOffsetsOnly){
        Utils.setCamusOffsetsInZK
        0
      } else {

        if (job.waitForCompletion(true)) {
          successCallback
          0
        } else {
          errorCallback
          1
        }
      }
    } else {
      println("Error setting the Camus2Kafka configuration, please check your configuration")
      2
    }
  }

  def successCallback {

    Utils.setCamusOffsetsInZK match {
      case true => println("Yay, for real!")
      case false => println("Error in successCallback: Could not set the offsets in ZK")
    }
  }

  def errorCallback {
    println("Booo!")
  }
}

class Camus2KafkaMapperByTime
  extends AbstractC2KMapper[LongWritable] {
  val longWritableKey = new LongWritable()

  def getOutputKey(key: AvroKey[GenericRecord], value: NullWritable): LongWritable = {
    longWritableKey.set(key.datum().get("time").asInstanceOf[Long])
    longWritableKey
  }
}

class Camus2KafkaReducerByTime
  extends AbstractC2KReducer[LongWritable] {
  def processBeforePublish(msg: Array[Byte])  = Utils.fromBinaryToJsonEncoded(msg)
}