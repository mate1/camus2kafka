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
import org.apache.avro.mapred._
import org.apache.avro.io.DatumReader
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.avro.mapreduce.{AvroJob, AvroKeyValueOutputFormat, AvroKeyInputFormat}


/**
 * Created with IntelliJ IDEA.
 * User: Boris Fersing
 * Date: 9/27/13
 * Time: 12:15 PM
 */

class Camus2KafkaJob extends Configured with Tool with Callback {

  val SCHEMA = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"RegUserTrackingLog\",\"namespace\":\"com.edate.data.dto.avro\",\"doc\":\"logging for user tracking on all registration gateways (Mate1 pages, 3rd party pages, mobile apps, mobile web...)\",\"fields\":[{\"name\":\"mate1_uuid\",\"type\":\"string\"},{\"name\":\"time\",\"type\":\"long\"},{\"name\":\"request_guid\",\"type\":\"string\"},{\"name\":\"legacy_log_type\",\"type\":\"int\"},{\"name\":\"cookie_guid\",\"type\":\"string\"},{\"name\":\"value\",\"type\":\"string\"},{\"name\":\"user_agent\",\"type\":[\"null\",\"string\"],\"default\":\"null\"},{\"name\":\"mobile_version\",\"type\":[\"null\",\"string\"],\"default\":\"null\"},{\"name\":\"ip_address\",\"type\":[\"null\",\"string\"],\"default\":\"null\"},{\"name\":\"query_string\",\"type\":[\"null\",\"string\"],\"default\":\"null\"}]}");

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

/*    val avroFile = new Path(args(0))
    val input : SeekableInput = new FsInput(avroFile, conf)

    val datumReader : DatumReader[GenericRecord] = new GenericDatumReader[GenericRecord]()
    val fileReader : FileReader[GenericRecord] = DataFileReader.openReader(input, datumReader)

    val record : GenericData.Record = new GenericData.Record(fileReader.getSchema)

    while(fileReader.hasNext){
      fileReader.next(record)
      println(record.get("user_agent"))
      println(record.get("ip_address"))
    }

    fileReader.close()*/

    val job = new Job(conf, "word count")
    job.setJarByClass(classOf[AvroReaderMapper])
    job.setMapperClass(classOf[AvroReaderMapper])
    //job.setCombinerClass(classOf[SendToKafkaReducer])
    job.setReducerClass(classOf[SendToKafkaReducer])

    job.setOutputKeyClass(classOf[LongWritable])
    job.setOutputValueClass(classOf[Text])

    job.setMapOutputKeyClass(classOf[LongWritable])
    job.setMapOutputValueClass(classOf[Text])

    job.setInputFormatClass(classOf[AvroKeyInputFormat[GenericRecord]])
    //job.setOutputFormatClass(classOf[AvroKeyValueOutputFormat[LongWritable, AvroValue[GenericRecord]]])

    job.setReduceSpeculativeExecution(false)
    //job.setNumReduceTasks(0)
    //AvroJob.setOutputValueSchema(job, SCHEMA)

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


class AvroReaderMapper extends Mapper[AvroKey[GenericRecord], NullWritable, LongWritable, Text]{

  val avroValue = new AvroValue[GenericRecord](null)
  val genericData = GenericData.get()

  override def map(key: AvroKey[GenericRecord], value: NullWritable,
                   context: Mapper[AvroKey[GenericRecord], NullWritable, LongWritable, Text]#Context) {

    val datum = key.datum()
    val time = datum.get("time").toString.toLong
    val schema = datum.getSchema
    val record = genericData.newRecord(datum, schema).asInstanceOf[GenericRecord]
    avroValue.datum(record)

    val txt = new Text(datum.toString)

    println(record.getSchema)
    
    context.write(new LongWritable(time), txt)
  }
}

class SendToKafkaReducer extends Reducer[LongWritable, Text, LongWritable, Text]{
  override def reduce(key: LongWritable, values: Iterable[Text],
                      context: Reducer[LongWritable, Text, LongWritable, Text]#Context) {

    values.asScala.foreach(value => context.write(key,value))
  }
}