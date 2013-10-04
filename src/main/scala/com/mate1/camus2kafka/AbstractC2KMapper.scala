package com.mate1.camus2kafka

import org.apache.hadoop.mapreduce.Mapper
import org.apache.avro.generic.{GenericRecord, GenericData}
import org.apache.hadoop.io.{NullWritable, BytesWritable}
import org.apache.avro.mapred.AvroKey

/**
 * Created with IntelliJ IDEA.
 * User: Boris Fersing
 * Date: 10/4/13
 * Time: 9:28 AM
 */

/**
 * Abstract class for the C2K Mapper
 * @tparam INKEY
 * @tparam INVALUE
 * @tparam OUTKEY
 * @tparam OUTVALUE
 */
abstract class AbstractC2KMapper[INKEY, INVALUE, OUTKEY, OUTVALUE]
  extends Mapper[INKEY, INVALUE, OUTKEY, OUTVALUE]
  with C2KJobConfig{

  // That type definition make the code easier to read
  type MapperContext = Mapper[INKEY, INVALUE, OUTKEY, OUTVALUE]#Context

  /**
   * Initializes the C2KJobConfig
   * @param context
   */
  override def setup(context: MapperContext) {
    super.setup(context)
    initConfig(context.getConfiguration)
  }

  /**
   * A default implementation of the map method that uses the getOutputKey and getOutputValue methods
   * @param key
   * @param value
   * @param context
   */
  override def map(key: INKEY, value: INVALUE, context: MapperContext) {

    context.write(getOutputKey(key,value), getOutputValue(key,value))
  }

  /**
   * Provide the output key that will be written to the Mapper's context and sent to the Reducer
   * @param key The input key
   * @param value The input value
   * @return The output key that will be sent to the Reducer
   */
  def getOutputKey(key: INKEY, value: INVALUE) : OUTKEY

  /**
   * Provide the output value that will be written to the Mapper's context and sent to the Reducer
   * @param key The input key
   * @param value The input value
   * @return The output value that will be sent to the Reducer
   */
  def getOutputValue(key: INKEY, value: INVALUE) : OUTVALUE

}
