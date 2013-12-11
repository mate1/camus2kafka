package com.mate1.camus2kafka.mapper

import com.mate1.camus2kafka.AbstractC2KMapper
import org.apache.hadoop.io.{NullWritable, LongWritable}
import org.apache.avro.mapred.AvroKey
import org.apache.avro.generic.GenericRecord

class HeaderTimeBasedC2KMapper
  extends AbstractC2KMapper[LongWritable] {
  val longWritableKey = new LongWritable()

  def getOutputKey(key: AvroKey[GenericRecord], value: NullWritable): LongWritable = {
    longWritableKey.set(key.datum().get("header").asInstanceOf[GenericRecord].get("time").asInstanceOf[Long])
    longWritableKey
  }
}