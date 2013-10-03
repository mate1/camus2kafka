package com.mate1.camus2kafka

import org.apache.hadoop.util.Tool
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.mapreduce.{Job, Reducer, Mapper}
import java.lang.Iterable
import scala.collection.JavaConverters._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat
import scala.Predef._
import java.io.ByteArrayOutputStream
import org.apache.avro.mapred._
import org.apache.avro.io.{BinaryDecoder, DecoderFactory, EncoderFactory}
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericData, GenericRecord}
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

    if (validateConfig(conf)){

      val job = new Job(conf, "Camus to Kafka")

      FileInputFormat.addInputPath(job, new Path(conf.get(C2KJobConfig.INPUT_PATH)))

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
      2
    }
  }

  override def successCallback {
    super.successCallback
  }

  override def errorCallback {
    super.errorCallback
  }
}


class Camus2KafkaMapper
  extends Mapper[AvroKey[GenericRecord], NullWritable, LongWritable, BytesWritable]
  with C2KJobConfig {

  type MapperContext = Mapper[AvroKey[GenericRecord], NullWritable, LongWritable, BytesWritable]#Context

  val bytesWritableValue = new BytesWritable()
  val bytesWritableKey = new BytesWritable()
  val genericData = GenericData.get()


  override def setup(context: MapperContext) {
    super.setup(context)
    initConfig(context.getConfiguration)
  }


  override def map(key: AvroKey[GenericRecord], value: NullWritable, context: MapperContext) {

    val datum = key.datum()
    val time = datum.get("time").asInstanceOf[Long]

    val out = new ByteArrayOutputStream()
    val writer = new GenericDatumWriter[GenericRecord](C2KJobConfig.outputSchema)

    val encoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(datum, encoder)
    encoder.flush()
    out.close()

    val bytesArray = out.toByteArray

    bytesWritableValue.set(bytesArray, 0 , bytesArray.length-1)

    context.write(new LongWritable(time), bytesWritableValue)
  }
}

class Camus2KafkaReducer
  extends Reducer[LongWritable, BytesWritable, LongWritable, BytesWritable]
  with C2KJobConfig {

  type ReducerContext = Reducer[LongWritable, BytesWritable, LongWritable, BytesWritable]#Context


  override def setup(context: ReducerContext) {
    super.setup(context)
    initConfig(context.getConfiguration)
  }

  override def reduce(key: LongWritable, values: Iterable[BytesWritable], context: ReducerContext) {
    val datumReader : GenericDatumReader[GenericRecord] = new GenericDatumReader(C2KJobConfig.outputSchema)
    val record : GenericData.Record = new GenericData.Record(C2KJobConfig.outputSchema)
    val decoderFactory : DecoderFactory = DecoderFactory.get()
    val decoder : BinaryDecoder = decoderFactory.binaryDecoder(Array[Byte](), null)

    values.asScala.foreach(value => {

      datumReader.read(record, decoderFactory.binaryDecoder(value.getBytes, decoder))
      
      println(key.toString+"\t\t"+ record.get("user_agent"))
      context.write(key,value)
    })
  }
}