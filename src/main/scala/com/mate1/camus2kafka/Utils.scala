package com.mate1.camus2kafka

import kafka.producer.{ProducerData, ProducerConfig, Producer}
import kafka.message.Message
import java.util.Properties
import org.apache.avro
import org.apache.avro.io.{BinaryDecoder, DecoderFactory, EncoderFactory}
import org.apache.avro.generic.{GenericDatumWriter, GenericData, GenericRecord, GenericDatumReader}
import java.io.ByteArrayOutputStream
import org.apache.avro.specific.{SpecificRecord, SpecificDatumWriter}

/**
 * Created with IntelliJ IDEA.
 * User: Boris Fersing
 * Date: 10/3/13
 * Time: 3:53 PM
 */
object Utils {

  /**
   * Creates a producer for the given topic
   */
  private def createKafkaProducer : Option[Producer[String, Message]] = {

    val producer = try {

      val zkHosts = C2KJobConfig.config.get(C2KJobConfig.ZK_HOSTS)

      // The producer properties
      val producerProps = new Properties()

      producerProps.put("zk.connect", zkHosts)
      producerProps.put("producer.type", "async")
      producerProps.put("batch.size", "100")
      producerProps.put("queue.time", "1000")

      val producerConfig = new ProducerConfig(producerProps)

      Some(new Producer[String, Message](producerConfig))

    } catch {
      case e: Exception => {
        println("Could not create kafka producer")
        None
      }
    }
    producer
  }


  def publishToKafka(msg: Array[Byte]) = {
    val topic = C2KJobConfig.config.get(C2KJobConfig.KAFKA_TOPIC)

    try {

      createKafkaProducer match {
        case None => println("Failed to publish")
        case producer => {
          producer.get.send(new ProducerData(topic, new Message(msg)))
          println("Publishing ok")
        }
      }

    } catch {
      case e: Exception => {
        // happens when trying to publish and kafka suddenly switched off,
        // but not when zookeeper is off. When zookeeper comes back on,
        // all the publishes were queued will all fire
        println("Unable to publish an avro message from producer for topic: " + topic)
        e.printStackTrace()

      }
    }
  }

  def convertToJSONEncoded(msg: Array[Byte]) : Array[Byte]= {

    val schema = C2KJobConfig.outputSchema

    // Decode to a record
    val datumReader : GenericDatumReader[GenericRecord] = new GenericDatumReader(schema)
    val record : GenericData.Record = new GenericData.Record(schema)
    val decoderFactory : DecoderFactory = DecoderFactory.get()
    val decoder : BinaryDecoder = decoderFactory.binaryDecoder(Array[Byte](), null)
    datumReader.read(record, decoderFactory.binaryDecoder(msg, decoder))

    // Re-encode in JsonEncoded
    val out = new ByteArrayOutputStream()
    val writer = new GenericDatumWriter[GenericRecord](schema)
    val encoder = EncoderFactory.get().jsonEncoder(schema, out)
    writer.write(record, encoder)
    encoder.flush()
    out.close()

    out.toByteArray
  }
}
