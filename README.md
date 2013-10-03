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

