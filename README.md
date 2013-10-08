# Camus2Kafka

## Introduction

This tool lets you take Kafka topics that were previously persisted in Hadoop through Camus and push them back into Kafka.

## Building

Assuming you already have SBT installed:

    $ git clone git://github.com/mate1/camus2kafka.git
    $ cd camus2kafka
    $ sbt assembly

The 'fat jar' is now available as:

    target/camus2kafka-project-0.0.1.jar

## Running

    $ hadoop jar target/camus2kafka-project-0.0.1.jar	

## Configuration
Check the [example configuration file](http://github.com/mate1/camus2kafka/blob/master/example_conf.xml) for the supported parameters. The properties in the configuration file can be overriden using the **-D name=value** command line argument
