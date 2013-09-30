package com.mate1.camus2kafka

import org.apache.hadoop.util.Tool
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.mapreduce.{Job, Reducer, Mapper}
import java.lang.Iterable
import scala.collection.JavaConverters._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.output.{NullOutputFormat, FileOutputFormat}
import scala.Predef._
import org.apache.avro.Schema
import java.io.File
import org.apache.avro.file.{DataFileReader, FileReader, SeekableInput}
import org.apache.avro.mapred.{AvroKey, FsInput}
import org.apache.avro.io.DatumReader
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.hadoop.io.NullWritable


/**
 * Created with IntelliJ IDEA.
 * User: Boris Fersing
 * Date: 9/27/13
 * Time: 12:15 PM
 */

class Camus2KafkaJob extends Configured with Tool with Callback {

  private def parseSchemaFromFile(schemaFile: File) : Schema = {
    new Schema.Parser().parse(schemaFile)
  }

  def run(args: Array[String]): Int = {
    val conf = getConf

    if (conf.getBoolean("printconf", false)){
      conf.asScala.foreach(entry => println(entry.getKey+" : "+entry.getValue))
      println("\n=============\nJob Custom Params:\n")
      args.foreach(println)
      println("\n")
    }

    if (args.length < 3) {
      println("Usage: wordcount <in> <out> <inputSchema> (<outputSchema>)")
      return 2
    }

    val avroFile = new Path(args(0))
    val input : SeekableInput = new FsInput(avroFile, conf)

    val datumReader : DatumReader[GenericRecord] = new GenericDatumReader[GenericRecord]()
    val fileReader : FileReader[GenericRecord] = DataFileReader.openReader(input, datumReader)

    val record : GenericData.Record = new GenericData.Record(fileReader.getSchema)

    while(fileReader.hasNext){
      fileReader.next(record)
      println(record.get("user_agent"))
      println(record.get("ip_address"))
    }

    fileReader.close()

    val job = new Job(conf, "word count")
    job.setJarByClass(classOf[AvroReaderMapper])
    job.setMapperClass(classOf[AvroReaderMapper])
    job.setCombinerClass(classOf[SendToKafkaReducer])
    job.setReducerClass(classOf[SendToKafkaReducer])
    job.setOutputKeyClass(classOf[NullOutputFormat[Object,Object]])
    job.setOutputValueClass(classOf[NullOutputFormat[Object,Object]])
    //job.setInputFormatClass()
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


class AvroReaderMapper extends Mapper[AvroKey[GenericRecord], NullWritable, AvroKey[GenericRecord], AvroKey[GenericRecord]]{
  override def map(key: AvroKey[GenericRecord], value: NullWritable,
                   context: Mapper[AvroKey[GenericRecord], NullWritable, AvroKey[GenericRecord], AvroKey[GenericRecord]]#Context) {

    context.write(key.asInstanceOf[AvroKey[GenericRecord]],value.asInstanceOf[AvroKey[GenericRecord]])
  }
}

class SendToKafkaReducer extends Reducer[AvroKey[GenericRecord], AvroKey[GenericRecord], NullWritable, NullWritable]{
  override def reduce(key: AvroKey[GenericRecord], values: Iterable[AvroKey[GenericRecord]],
                      context: Reducer[AvroKey[GenericRecord], AvroKey[GenericRecord], NullWritable, NullWritable]#Context) {

    values.asScala.foreach(value => context.write(key.asInstanceOf[NullWritable],value.asInstanceOf[NullWritable]))
  }
}