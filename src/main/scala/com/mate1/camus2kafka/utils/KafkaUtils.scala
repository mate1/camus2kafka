package com.mate1.camus2kafka.utils

import com.mate1.camus2kafka.C2KJobConfig

/**
 * Created with IntelliJ IDEA.
 * User: borisf
 * Date: 10/10/13
 * Time: 10:58 AM
 */

/**
 * Kafka / Zookeeper tools
 */

trait KafkaUtils {
  def publishToKafka(msg: Array[Byte])
  def setCamusOffsetsInZK() : Boolean
}

object KafkaUtils {

  def apply() : KafkaUtils = new KafkaUtilsImp()


  private class KafkaUtilsImp  extends KafkaUtils {
    import kafka.producer.{ProducerData, ProducerConfig, Producer}
    import kafka.message.Message
    import java.util.Properties
    import org.apache.hadoop.io.SequenceFile
    import org.apache.hadoop.io.SequenceFile.Reader
    import org.apache.hadoop.fs._
    import com.linkedin.camus.etl.kafka.common.EtlKey
    import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooKeeper}
    import org.apache.zookeeper.Watcher.Event
    import akka.actor.ActorSystem
    import akka.agent.Agent
    import org.apache.zookeeper.KeeperException.NodeExistsException
    import scala.collection.mutable
    import scala.Some
    import org.apache.zookeeper.data.Stat


    /**
     * Creates a Kafka producer
     */
    private lazy val kafkaProducer : Option[Producer[String, Message]] = {

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
     * @param msg The message to publish
     */
    def publishToKafka(msg: Array[Byte]) = {
      val topic = C2KJobConfig.replayTopic

      try {

        kafkaProducer match {
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
     * Look for the latest offset files, parse them and set the latest offsets for the topic in ZK
     *
     * @return true if successful, false otherwise
     */
    def setCamusOffsetsInZK() : Boolean = try {

      val etlKey = new EtlKey()

      getZookeeper match {
        case Some(zk) => {
          val etlkeys = new mutable.MutableList[EtlKey]()

          getOffsetsFiles.foreach(file => {
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
    private def getRemoteOffsetsFiles(fs: FileSystem) : Option[Array[FileStatus]] = {
      fs.listStatus(new Path(C2KJobConfig.camusHistoryDir)).last match {
        case null => None
        case dir => Some(fs.listStatus(dir.getPath, C2KJobConfig.offsetsFilter))
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

      // Get the current ACLs for /consumers and reuse them
      val acls = zk.getACL("/consumers", new Stat())

      zk.create(C2KJobConfig.zkConsumerPath, null, acls, CreateMode.PERSISTENT)
      zk.create(C2KJobConfig.zkConsumerPath + "/offsets", null, acls, CreateMode.PERSISTENT)
      zk.create(C2KJobConfig.zkOffsetsPath, null, acls, CreateMode.PERSISTENT)

      etlkeys.foreach( key => {
        val fullPath = C2KJobConfig.zkOffsetsPath+"/"+key.getNodeId+"-"+key.getPartition

        zk.create(fullPath, null, acls, CreateMode.PERSISTENT)
        zk.setData(fullPath, key.getOffset.toString.getBytes, -1)
        println(fullPath + " : "+key.getOffset)
      })

    } catch {
      case e: NodeExistsException => {
        println("Could not write the offsets to ZK. Node already exists: "+e.getLocalizedMessage)
      }
      case e : Exception => throw e
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
     * Copy the offsets files locally and return them.
     * @return the local offsets files
     */
    private def getOffsetsFiles = {
      val hdfs = FileSystem.get(C2KJobConfig.config)
      val localFs = FileSystem.getLocal(C2KJobConfig.config)

      val localTmpDir = new Path(C2KJobConfig.localTmpDir)

      // The actual copy procedure
      def doCopy() = {
        println("Copying offsets files to the local temp dir...")
        getRemoteOffsetsFiles(hdfs) match {
          case None => println("No offset files found.")
          case Some(files) => files.foreach(file => {
            hdfs.copyToLocalFile(file.getPath, localTmpDir)
          })
        }
        println("Copy done!")
      }

      // Only copy the files if the target dir doesn't exist or is empty
      localFs.exists(localTmpDir) match {
        case true => localFs.listStatus(localTmpDir, C2KJobConfig.offsetsFilter).isEmpty match {
          case false => println("Local temp dir not empty, working with the existing files")
          case true => doCopy()
        }
        case false => {
          localFs.mkdirs(localTmpDir)
          doCopy()
        }
      }

      // Return the local files for further processing
      localFs.listStatus(localTmpDir)
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
}