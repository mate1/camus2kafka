package com.mate1.camus2kafka.reducer

import com.mate1.camus2kafka.{AbstractC2KReducer}
import org.apache.hadoop.io.LongWritable
import com.mate1.camus2kafka.utils.DecodingUtils

/**
 * Created with IntelliJ IDEA.
 * User: borisf
 * Date: 10/10/13
 * Time: 9:35 AM
 */
class JsonC2KReducer
  extends AbstractC2KReducer[LongWritable] {

  val decodingUtils = DecodingUtils()

  def processBeforePublish(msg: Array[Byte])  = decodingUtils.fromBinaryToJsonEncoded(msg)
}