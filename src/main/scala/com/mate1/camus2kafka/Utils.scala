package com.mate1.camus2kafka

import kafka.producer.{ProducerData, ProducerConfig, Producer}
import kafka.message.Message
import java.util.Properties
import org.apache.avro.io.{BinaryDecoder, DecoderFactory, EncoderFactory}
import org.apache.avro.generic.{GenericDatumWriter, GenericData, GenericRecord, GenericDatumReader}
import java.io.ByteArrayOutputStream
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.SequenceFile.Reader
import org.apache.hadoop.fs.{FileSystem, FileStatus, GlobFilter, Path}
import com.linkedin.camus.etl.kafka.common.EtlKey
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}
import org.apache.zookeeper.Watcher.Event
import akka.actor.ActorSystem
import akka.agent.Agent
import org.apache.zookeeper.KeeperException.NoNodeException
import scala.collection.mutable

/**
 * Created with IntelliJ IDEA.
 * User: Boris Fersing
 * Date: 10/3/13
 * Time: 3:53 PM
 */
object Utils {

  /**
   * Creates a Kafka producer
   */
  private def createKafkaProducer : Option[Producer[String, Message]] = {

    val producer = try {

      val zkHosts = C2KJobConfig.zkHosts

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


  /**
   * Publish a message to the replay topic
   *
   * @param msg
   */
  def publishToKafka(msg: Array[Byte]) = {
    val topic = C2KJobConfig.replayTopic

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

  /**
   * Convert an array of bytes of a binary encoded message to a an array of bytes of Json encoded message
   *
   * @param msg Array of bytes from a binary encoded message
   * @return Array of bytes from a Json encoded message
   */
  def fromBinaryToJsonEncoded(msg: Array[Byte]) : Array[Byte]= {

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

  /**
   * Look for the latest offset files, parse them and set the latest offsets for the topic in ZK
   *
   * @return true if successful, false otherwise
   */
  def setCamusOffsetsInZK() : Boolean = try {
    val fs = new Path(C2KJobConfig.camusHistoryDir).getFileSystem(C2KJobConfig.config)
    val etlKey = new EtlKey()

    getZookeeper match {
      case Some(zk) => {
        getOffsetsFiles(fs) match {
          case None => {
            println("No offset files found.")
            zk.close()
            false
          }
          case Some(files) => {
            val etlkeys = new mutable.MutableList[EtlKey]()

            files.foreach(file => {
              val reader : SequenceFile.Reader = new Reader(C2KJobConfig.config, Reader.file(file.getPath))

              while (reader.next(etlKey)) {
                etlKey.getTopic match {
                  case topic if topic == C2KJobConfig.sourceTopic => etlkeys += new EtlKey(etlKey)
                  case _ => ()
                }
              }
              reader.close()
            })
            writeOffsetsInZK(etlkeys, zk)
            zk.close()
            true
          }
        }
      }
      case None => {
        println("Could not connect to ZK!")
        false
      }
    }
  } catch {
    case e: Exception => {
      e.printStackTrace()
      false
    }
  }

  /**
   * Search the Camus history directory for Camus offset files and return them
   *
   * @param fs The Filesystem that contains the history dir
   * @return An Array of FileStatus
   */
  private def getOffsetsFiles(fs: FileSystem) : Option[Array[FileStatus]] = {
    fs.listStatus(new Path(C2KJobConfig.camusHistoryDir)).last match {
      case null => None
      case dir => Some(fs.listStatus(dir.getPath, new GlobFilter("offsets-m-*")))
    }
  }

  /**
   * Retrieve the offset information for the etlKey
   * and write the offset to ZK using the provided ZK connection.
   *
   * @param etlkeys the Camus etlKeys that contains the offset (and other information such as nodeId, partition, ...)
   * @param zk The Zookeeper connection used to write the information
   */
  private def writeOffsetsInZK(etlkeys : Seq[EtlKey], zk: ZooKeeper) = try {
    val offsetsPath = "/consumers/%s/offsets/%s".format(C2KJobConfig.consumerGroup, C2KJobConfig.sourceTopic)

    offsetPathAlreadyExists(offsetsPath, zk) match {
      case true => println("The Consumer Group / Topic combination already has offsets in ZK. Aborting")
      case false => etlkeys.foreach( key => {
        zk.setData(offsetsPath+"/"+key.getNodeId+"-"+key.getPartition, key.getOffset.toString.getBytes, -1)
        println(offsetsPath+"\t"+key.getOffset)
      })

    }
  } catch {
    case e: Exception => {
      println("Trying to recover")
      throw e
    }
  }

  /**
   * Check if the offset path already exists in ZK
   * @param path
   * @param zk
   * @return true if the path already exists, false otherwise
   */
  private def offsetPathAlreadyExists(path: String, zk: ZooKeeper) : Boolean = try {
    zk.getData(path, false, null)
    true
  } catch {
    case e: NoNodeException => false
  }

  /**
   * Return a zookeeper connection
   *
   * @return Some(zk) if the connection was successful, None otherwise
   */
  private def getZookeeper : Option[ZooKeeper] = {
    implicit val system = ActorSystem("Camus2KafkaUtils")
    val connected = Agent(false)
    val zk = new ZooKeeper(C2KJobConfig.zkHosts, 10000, new ZkWatcher(connected))
    var retry = 10

    while (!connected.get() && retry > 0) {
      Thread.sleep(100)
      retry = retry - 1
    }

    connected.get() match {
      case true => {
        system.shutdown()
        Some(zk)
      }
      case false => {
        println("connectToZk: Failed to connect to zookeeper on %s".format(C2KJobConfig.zkHosts))
        system.shutdown()
        None
      }
    }
  }

  /**
   * A very simple ZooKeeper watcher that notifies
   * its caller by means of an Agent that the
   * connection to ZooKeeper is now ready.
   */
  private class ZkWatcher(connected: Agent[Boolean]) extends Watcher {

    def process(event: WatchedEvent) {
      event.getType match {
        case Event.EventType.None if event.getState == Event.KeeperState.SyncConnected => {
          // We are are being told that the state of the connection has changed to SyncConnected
          connected.send(true)
        }

        case _ =>
      }
    }
  }

}
