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

import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.io.{Writable, NullWritable, BytesWritable}
import java.lang.Iterable
import scala.collection.JavaConverters._
import com.mate1.camus2kafka.utils.KafkaUtils

/**
 * Created with IntelliJ IDEA.
 * User: Boris Fersing
 * Date: 10/4/13
 * Time: 9:28 AM
 */

abstract class AbstractC2KReducer[INKEY <: Writable]
  extends Reducer[INKEY, BytesWritable, NullWritable, NullWritable]
  with C2KJobConfig{

  val kafkaUtils = KafkaUtils()

  type ReducerContext = Reducer[INKEY, BytesWritable, NullWritable, NullWritable]#Context

  override def setup(context: ReducerContext) {
    super.setup(context)
    initConfig(context.getConfiguration)
  }

  override def reduce(key: INKEY, values: Iterable[BytesWritable], context: ReducerContext) {
    values.asScala.foreach(value => publish(value.getBytes))
  }

  def publish(msg: Array[Byte]) = {
    kafkaUtils.publishToKafka(processBeforePublish(msg))
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
