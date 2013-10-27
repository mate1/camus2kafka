# Camus2Kafka

## Introduction

This tool lets you take Kafka topics that were previously persisted in Hadoop through Camus and push them back into Kafka.

### Background

At Mate1, we have Kafka consumers that process "raw" Kafka topics coming out of various types of servers, either to push them into databases, or into "enriched" versions of the same topics (with added metadata, for example). In some cases, we make updates to our Kafka consumers that warrant re-processing all of our legacy raw data (because we are infering new types of metadata from the raw data, for example).

We do not want to keep the data for all our topics inside Kafka forever (we instead use Camus and Hadoop for that), but we are willing to re-publish a single complete topic back into Kafka if need be. This topic can be re-published into a replay version of the original topic for the sole purpose of being re-consumed. If disk footprint was a concern, we could even set an aggressively low retention time on the replay topic so that it gets cleaned up by Kafka before the whole topic is done re-publishing, but that has not been a concern for us so far.

This way of doing things allows us to re-use the same consumer code we normally use to process topics in a streaming fashion, but for the whole topic, rather than needing to adapt our consumer code to custom Map Reduce jobs whose sole purpose would be a one time re-processing run.

We are open-sourcing camus2kafka for other people who may have similar needs. It should be fairly easy to extend camus2kafka so that it uses other fields to sort the input data with, as well as alternative formats to re-publish the data as.

### How it works

At a high-level, camus2kafka is an sbt project that spawns a Map Reduce job, and then performs some extra operations once the MR job is finished.

#### Map Reduce job

The MR job has a few purposes: reading data from HDFS, sorting it and republishing it into Kafka with a specified avro schema.

The mapper tasks read an HDFS directory containing a topic persisted using Camus. They then decode the AvroSequenceFiles using the schema that is embedded in their header, and look for a time field within the extracted GenericRecords. The mappers then emit key/value pairs where the key is the extracted time field, and the value is a BytesWritable representing the GenericRecord re-serialized using the specified avro schema (so that schema evolution does not need to be taken into account on a per-record level by the reducer).

The shuffling phase takes care of sorting the data by the provided key and feeds it into the reducer.

Finally, the reducer task ingests data, and simply republishes it into Kafka in a replay topic (which, by default, is the original topic's name with "_REPLAY" appended to it).

#### Post-MR processing

After the MR job is finished, camus2kafka will look in Camus' execution directory on HDFS for the offset files of the latest Camus run, and copy those files locally. It will then decode the contents of each offset file so that it can find the Kafka offsets that correspond to the correct cut-off point in the original Kafka topic. Finally, it will commit those offsets into Zookeeper for the consumer group name that was passed in parameter (but only if that ZK path is currently non-existent, so it will not overwrite existing consumer groups' offsets).

#### How to use the output of camus2kafka

After camus2kafka is done running, you can spawn one or many high-level Kafka consumers to pull and process the replay topic written by camus2kafka. Once those consumers are done consuming the replay topic, they can be switched off and reconfigured to pull from the original topic name using the consumer group that was provided to camus2kafka. This last step ensures that the consumers resume pulling from the original topic at the exact offsets that the last Camus run stopped at.

## Building

Assuming you already have SBT installed:

    $ git clone git://github.com/mate1/camus2kafka.git
    $ cd camus2kafka
    $ ./sbt assembly

The 'fat jar' is now available as:

    target/camus2kafka-project-0.0.1.jar

## Running

    $ hadoop jar target/camus2kafka-project-0.0.1.jar -conf config_file.xml

## Configuration

Check the [example configuration file](http://github.com/mate1/camus2kafka/blob/master/example_conf.xml) for the supported parameters. The properties in the configuration file can be overriden using the **-D name=value** command line argument
