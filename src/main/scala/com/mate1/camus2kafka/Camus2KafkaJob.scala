package com.mate1.camus2kafka

import org.apache.hadoop.util.Tool
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.mapreduce.{Job, Reducer, Mapper}
import java.lang.Iterable
import scala.collection.JavaConverters._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.mapreduce.lib.output.{NullOutputFormat, FileOutputFormat}
import scala.Predef._
import java.io.{ByteArrayOutputStream}
import org.apache.avro.mapred._
import org.apache.avro.io.EncoderFactory
import org.apache.avro.generic.{GenericDatumWriter, GenericData, GenericRecord}
import org.apache.hadoop.io.{BytesWritable, LongWritable, NullWritable}
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.hadoop.hdfs.DistributedFileSystem
import scala.io.Source
import org.apache.avro.Schema


/**
 * Created with IntelliJ IDEA.
 * User: Boris Fersing
 * Date: 9/27/13
 * Time: 12:15 PM
 */

class Camus2KafkaJob extends Configured with Tool with Callback {

  def run(args: Array[String]): Int = {
    val conf = getConf

    if (conf.getBoolean("printconf", false)){
      conf.asScala.foreach(entry => println(entry.getKey+" : "+entry.getValue))
      println("\n=============\nJob Custom Params:\n")
      args.foreach(println)
      println("\n")
    }

    if (args.length < 3) {
      println("Usage: wordcount <in> <out> <outputSchema>")
      return 2
    }

    val outputSchemaFile = new Path(args(2))
    val fs : FileSystem = outputSchemaFile.getFileSystem(conf)
    val inputStream : FSDataInputStream = fs.open(outputSchemaFile)
    val SCHEMA = Source.fromInputStream(inputStream).mkString
    inputStream.close()
    conf.setStrings("AVRO_OUTPUT_SCHEMA", SCHEMA)

    
    val job = new Job(conf, "Camus to Kafka")

    job.setJarByClass(classOf[AvroReaderMapper])
    job.setMapperClass(classOf[AvroReaderMapper])
    job.setReducerClass(classOf[SendToKafkaReducer])

    job.setOutputKeyClass(classOf[LongWritable])
    job.setOutputValueClass(classOf[BytesWritable])

    job.setMapOutputKeyClass(classOf[LongWritable])
    job.setMapOutputValueClass(classOf[BytesWritable])
    job.setOutputFormatClass(classOf[NullOutputFormat[LongWritable, BytesWritable]])

    job.setInputFormatClass(classOf[AvroKeyInputFormat[GenericRecord]])

    job.setReduceSpeculativeExecution(false)

    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    if (job.waitForCompletion(true)) 0 else 1
  }

  override def successCallback {
    super.successCallback
  }

  override def errorCallback {
    super.errorCallback
  }
}


class AvroReaderMapper extends Mapper[AvroKey[GenericRecord], NullWritable, LongWritable, BytesWritable]{

  type AvroReaderContext = Mapper[AvroKey[GenericRecord], NullWritable, LongWritable, BytesWritable]#Context

  val bytesWritableValue = new BytesWritable()
  val bytesWritableKey = new BytesWritable()
  val genericData = GenericData.get()

  var outputSchema : Option[Schema] = None

  def getOutputSchema(context: AvroReaderContext) = outputSchema match {
    case Some(schema) => schema
    case None => {
      outputSchema = Some(Schema.parse(context.getConfiguration.get("AVRO_OUTPUT_SCHEMA")))
      outputSchema.get
    }
  }

  override def map(key: AvroKey[GenericRecord], value: NullWritable, context: AvroReaderContext) {

    val datum = key.datum()
    val time = datum.get("time").asInstanceOf[Long]

    val out = new ByteArrayOutputStream()
    val writer = new GenericDatumWriter[GenericRecord](getOutputSchema(context))

    val encoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(datum, encoder)
    encoder.flush()
    out.close()

    val bytesArray = out.toByteArray

    bytesWritableValue.set(bytesArray, 0 , bytesArray.length-1)

    context.write(new LongWritable(time), bytesWritableValue)
  }
}

class SendToKafkaReducer extends Reducer[LongWritable, BytesWritable, LongWritable, BytesWritable]{

  type SendToKafkaContext = Reducer[LongWritable, BytesWritable, LongWritable, BytesWritable]#Context

  var outputSchema : Option[Schema] = None

  def getOutputSchema(context: SendToKafkaContext) = outputSchema match {
    case Some(schema) => schema
    case None => {
      outputSchema = Some(Schema.parse(context.getConfiguration.get("AVRO_OUTPUT_SCHEMA")))
      outputSchema.get
    }
  }


  override def reduce(key: LongWritable, values: Iterable[BytesWritable], context: SendToKafkaContext) {

    values.asScala.foreach(value => {
      println(key.toString+"\t\t")
      context.write(key,value)
    })
  }
}