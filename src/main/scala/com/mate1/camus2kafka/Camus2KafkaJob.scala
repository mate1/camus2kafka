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

package com.mate1.camus2kafka

import org.apache.hadoop.util.Tool
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat
import scala.Predef._
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.avro.mapreduce.AvroKeyInputFormat
import com.mate1.camus2kafka.utils.KafkaUtils

/**
 * Created with IntelliJ IDEA.
 * User: Boris Fersing
 * Date: 9/27/13
 * Time: 12:15 PM
 */

class Camus2KafkaJob extends Configured with Tool with C2KJobConfig {

  val kafkaUtils = KafkaUtils()

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
        kafkaUtils.setCamusOffsetsInZK
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

    kafkaUtils.setCamusOffsetsInZK match {
      case true => println("Yay, for real!")
      case false => println("Error in successCallback: Could not set the offsets in ZK")
    }
  }

  def errorCallback {
    println("Booo!")
  }
}