# Camus2Kafka

## Introduction

This tool lets you take Kafka topics that were previously persisted in Hadoop through Camus and push them back into Kafka.

## Building

Assuming you already have SBT installed:

    $ git clone git://github.com/mate1/camus2kafka.git
    $ cd camus2kafka
    $ sbt assembly

The 'fat jar' is now available as:

    target/camus2kafka-0.0.1.jar

## Unit testing

The `assembly` command above runs the test suite - but you can also run this manually with:

    $ sbt test
    <snip>
    [info] + A WordCount job should
	[info]   + count words correctly
	[info] Passed: : Total 2, Failed 0, Errors 0, Passed 2, Skipped 0

## Running on Amazon EMR

### Prepare

Assuming you have already assembled the jarfile (see above), now upload the jar to Amazon S3.

Next, upload the data file [`data/hello.txt`] [hello-txt] to S3.

### Run

Finally, you are ready to run this job using the [Amazon Ruby EMR client] [emr-client]:

    $ elastic-mapreduce --create --name "camus2kafka" \
      --jar s3n://{{JAR_BUCKET}}/camus2kafka-0.0.1.jar \
      --arg com.mate1.camus2kafka.WordCountJob \
      --arg --hdfs \
      --arg --input --arg s3n://{{IN_BUCKET}}/hello.txt \
      --arg --output --arg s3n://{{OUT_BUCKET}}/results

Replace `{{JAR_BUCKET}}`, `{{IN_BUCKET}}` and `{{OUT_BUCKET}}` with the appropriate paths.

### Inspect

Once the output has completed, you should see a folder structure like this in your output bucket:

     results
     |
     +- _SUCCESS
     +- part-00000

Download the `part-00000` file and check that it contains:

	goodbye	1
	hello	1
	world	2

## Running on your own Hadoop cluster

If you are trying to run this on a non-Amazon EMR environment, you may need to edit:

    project/BuildSettings.scala

And comment out the Hadoop jar exclusions:

    // "hadoop-core-0.20.2.jar", // Provided by Amazon EMR. Delete this line if you're not on EMR
    // "hadoop-tools-0.20.2.jar" // "

## Next steps

Fork this project and adapt it into your own custom Scalding job.

To invoke/schedule your Scalding job on EMR, check out:

* [Spark Plug] [spark-plug] for Scala
* [Elasticity] [elasticity] for Ruby
* [Boto] [boto] for Python

## Roadmap

Nothing planned currently.

## Copyright and license

Copyright 2013 Mate1 Inc.
Copyright 2012-2013 Snowplow Analytics Ltd, with significant portions copyright 2012 Twitter, Inc.

Licensed under the [Apache License, Version 2.0] [license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[license]: http://www.apache.org/licenses/LICENSE-2.0
