package com.mate1.camus2kafka

import org.apache.hadoop.util.Tool
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.mapreduce.Job
import java.lang.Iterable
import scala.collection.JavaConverters._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat
import scala.Predef._
import java.io.ByteArrayOutputStream
import org.apache.avro.mapred._
import org.apache.avro.io.EncoderFactory
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.hadoop.io.{BytesWritable, LongWritable, NullWritable}
import org.apache.avro.mapreduce.AvroKeyInputFormat


/**
 * Created with IntelliJ IDEA.
 * User: Boris Fersing
 * Date: 9/27/13
 * Time: 12:15 PM
 */

class Camus2KafkaJob extends Configured with Tool with C2KJob{

  def run(args: Array[String]): Int = {
    val conf = getConf

    if (initConfig(conf)){
      val job = new Job(conf, "Camus to Kafka")

      FileInputFormat.addInputPath(job, new Path(C2KJobConfig.hdfsInputDir))

      job.setJarByClass(classOf[Camus2KafkaMapper])
      job.setMapperClass(classOf[Camus2KafkaMapper])
      job.setReducerClass(classOf[Camus2KafkaReducer])

      job.setInputFormatClass(classOf[AvroKeyInputFormat[GenericRecord]])

      job.setOutputKeyClass(classOf[LongWritable])
      job.setOutputValueClass(classOf[BytesWritable])
      job.setOutputFormatClass(classOf[NullOutputFormat[LongWritable, BytesWritable]])

      job.setReduceSpeculativeExecution(false)

      if (runJob(job)) 0 else 1

    } else {
      println("Error setting the Camus2Kafka configuration, please check your configuration")
      2
    }
  }

  override def successCallback {

    Utils.readCamusOffsets
    Utils.setCamusOffsetsInZK

    println("Yay!")
  }
}

class Camus2KafkaMapper
  extends AbstractC2KMapper[AvroKey[GenericRecord], NullWritable, LongWritable, BytesWritable]
  with C2KJobConfig {

  val bytesWritableValue = new BytesWritable()
  val longWritableKey = new LongWritable()


  def getOutputKey(key: AvroKey[GenericRecord], value: NullWritable): LongWritable = {
    val datum = key.datum()
    val time = datum.get("time").asInstanceOf[Long]

    longWritableKey.set(time)
    longWritableKey
  }

  def getOutputValue(key: AvroKey[GenericRecord], value: NullWritable): BytesWritable = {
    val datum = key.datum()

    val out = new ByteArrayOutputStream()
    val writer = new GenericDatumWriter[GenericRecord](C2KJobConfig.outputSchema)

    val encoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(datum, encoder)
    encoder.flush()
    out.close()

    val bytesArray = out.toByteArray

    bytesWritableValue.set(bytesArray, 0 , bytesArray.length-1)
    bytesWritableValue
  }
}

class Camus2KafkaReducer
  extends AbstractC2KReducer[LongWritable, BytesWritable, LongWritable, BytesWritable] {

  def processBeforePublish(msg: Array[Byte])  = Utils.fromBinaryToJsonEncoded(msg)

  override def reduce(key: LongWritable, values: Iterable[BytesWritable], context: ReducerContext) {
    values.asScala.foreach(value => publish(value.getBytes))
  }


}