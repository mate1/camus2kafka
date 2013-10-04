package com.mate1.camus2kafka

import org.apache.hadoop.mapreduce.Reducer

/**
 * Created with IntelliJ IDEA.
 * User: Boris Fersing
 * Date: 10/4/13
 * Time: 9:28 AM
 */

abstract class AbstractC2KReducer[INKEY, INVALUE, OUTKEY, OUTVALUE]
  extends Reducer[INKEY, INVALUE, OUTKEY, OUTVALUE]
  with C2KJobConfig{

  type ReducerContext = Reducer[INKEY, INVALUE, OUTKEY, OUTVALUE]#Context

  override def setup(context: ReducerContext) {
    super.setup(context)
    initConfig(context.getConfiguration)
  }

  def publish(msg: Array[Byte]) = {
    val processedMsg = processBeforePublish(msg)
    Utils.publishToKafka(processedMsg)
  }

  /**
   * That method takes an array of Bytes and return an array of bytes.
   * The returned value will be passed to Utils.publishToKafka(msg)
   *
   * This is where you might want to convert a BinaryEncoded GenericRecord into a JsonEncoded GenericRecord
   *
   * @param msg Array of Byte of a Binary encoded Generic Record
   * @return an Array of Byte
   */
  def processBeforePublish(msg: Array[Byte]) : Array[Byte]

}
