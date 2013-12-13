package com.mate1.camus2kafka.utils

import com.mate1.camus2kafka.C2KJobConfig

/**
 * Created with IntelliJ IDEA.
 * User: borisf
 * Date: 10/10/13
 * Time: 11:01 AM
 */
/**
 * Decoding / Encoding  Tools
 */

trait DecodingUtils {
  def fromBinaryToJsonEncoded(msg: Array[Byte]) : Array[Byte]
}

object DecodingUtils {

  def apply() : DecodingUtils = new DecodingUtilsImp()

  private class DecodingUtilsImp extends DecodingUtils {

    import org.apache.avro.io.{BinaryDecoder, DecoderFactory, EncoderFactory}
    import org.apache.avro.generic.{GenericDatumWriter, GenericData, GenericRecord, GenericDatumReader}
    import java.io.ByteArrayOutputStream

    private lazy val datumReader : GenericDatumReader[GenericRecord] = new GenericDatumReader(C2KJobConfig.outputSchema)
    private lazy val record : GenericData.Record = new GenericData.Record(C2KJobConfig.outputSchema)
    private lazy val decoderFactory : DecoderFactory = DecoderFactory.get()
    private lazy val reuseDecoder : BinaryDecoder = decoderFactory.binaryDecoder(Array[Byte](), null)
    private lazy val writer = new GenericDatumWriter[GenericRecord](C2KJobConfig.outputSchema)
    private lazy val out = new ByteArrayOutputStream()
    private lazy val encoder = EncoderFactory.get().jsonEncoder(C2KJobConfig.outputSchema, out)

    /**
     * Convert an array of bytes of a binary encoded message to a an array of bytes of Json encoded message
     *
     * @param msg Array of bytes from a binary encoded message
     * @return Array of bytes from a Json encoded message
     */
    def fromBinaryToJsonEncoded(msg: Array[Byte]) : Array[Byte] = {
      try {
        // Decode to a record
        datumReader.read(record, decoderFactory.binaryDecoder(msg, reuseDecoder))

        // Re-encode in JsonEncoded
        out.reset()
        writer.write(record, encoder)
        encoder.flush()

        // Return the re-encoded data
        out.toByteArray
      } catch {
        case e: Exception => {
          println("Got an exception in fromBinaryToJsonEncoded for the following byte array!" +
            "\n\nArray(" + msg.mkString(", ") + ")\n")
          throw e
        }
      }
    }
  }
}
