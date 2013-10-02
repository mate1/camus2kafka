package com.mate1.camus2kafka

import org.apache.hadoop.util.Tool
import org.apache.hadoop.conf.Configured
import java.lang.Iterable
import scala.collection.JavaConverters._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred._
import scala.Predef._
import org.apache.avro.Schema
import java.io.File
import org.apache.avro.file.{DataFileReader, FileReader, SeekableInput}
import org.apache.avro.mapred._
import org.apache.avro.mapred.{Pair => AvroPair}
import org.apache.avro.io.DatumReader
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}


/**
 * Created with IntelliJ IDEA.
 * User: Boris Fersing
 * Date: 9/27/13
 * Time: 12:15 PM
 */

class Camus2KafkaJobMapRed extends Configured with Tool with Callback {
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

  /*  val avroFile = new Path(args(0))
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

    val jobConf = new JobConf(conf, classOf[Camus2KafkaJobMapRed])

    jobConf.setJobName("Camus to Kafka")
    jobConf.setInputFormat(classOf[AvroInputFormat[GenericRecord]])

    AvroJob.setMapOutputSchema(jobConf, AvroPair.getPairSchema(Schema.create(Schema.Type.LONG), SCHEMA))
    AvroJob.setInputSchema(jobConf, SCHEMA)
    AvroJob.setOutputSchema(jobConf, SCHEMA)
    AvroJob.setMapperClass(jobConf, classOf[AvroReaderMapperMapRed])
    AvroJob.setReducerClass(jobConf, classOf[SendToKafkaReducerMapRed])
    AvroJob.setReflect(jobConf)

    FileInputFormat.setInputPaths(jobConf, new Path(args(0)))
    FileOutputFormat.setOutputPath(jobConf, new Path(args(1)))

    try {
    val job = JobClient.runJob(jobConf)

    job.waitForCompletion()

    if (job.isSuccessful) 0 else 1
    } catch {
      case e => {
        e.printStackTrace()
        1
      }
    }

  }

  override def successCallback {
    super.successCallback
  }

  override def errorCallback {
    super.errorCallback
  }
}

class AvroReaderMapperMapRed extends AvroMapper[GenericRecord, AvroPair[Long, GenericRecord]]{

  override def map(inputRecord: GenericRecord, collector: AvroCollector[AvroPair[Long, GenericRecord]], reporter: Reporter) {

    val time = inputRecord.get("time").toString.toLong
    collector.collect(new AvroPair[Long, GenericRecord](time, inputRecord))
  }
}

class SendToKafkaReducerMapRed extends AvroReducer[Long, GenericRecord, GenericRecord]{
  override def reduce(key: Long, values: Iterable[GenericRecord], collector: AvroCollector[GenericRecord], reporter: Reporter) {

    values.asScala.foreach(value => collector.collect(value))
  }
}
