package com.mate1.camus2kafka

import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.Tool
import scala.collection.JavaConverters._

/**
 * This class performs the map operation, translating raw input into the key-value
 * pairs we will feed into our reduce operation.
 */
class TokenizerMapper extends Mapper[Object,Text,Text,IntWritable] {
  val one = new IntWritable(1)
  val word = new Text

  override def map(key:Object, value:Text, context:Mapper[Object,Text,Text,IntWritable]#Context) = {
    for (t <-  value.toString().split("\\s")) {
      word.set(t)
      context.write(word, one)
    }
  }
}

/**
 * This class performs the reduce operation, iterating over the key-value pairs
 * produced by our map operation to produce a result. In this case we just
 * calculate a simple total for each word seen.
 */
class IntSumReducer extends Reducer[Text,IntWritable,Text,IntWritable] {
  override def reduce(key:Text, values:java.lang.Iterable[IntWritable], context:Reducer[Text,IntWritable,Text,IntWritable]#Context) = {
    val sum = values.asScala.foldLeft(0) { (t,i) => t + i.get }
    context.write(key, new IntWritable(sum))
  }
}

/**
 * The actual job class
 */
class  WordCountJob extends Configured with Tool with Callback {
  def run(args: Array[String]): Int = {

    val conf = getConf

    if (conf.getBoolean("printconf", false)){
      conf.asScala.foreach(entry => println(entry.getKey+" : "+entry.getValue))
      println("\n=============\nJob Custom Params:\n")
      args.foreach(println _)
      println("\n")
    }

    if (args.length != 2) {
      println("Usage: wordcount <in> <out>")
      return 2
    }

    val job = new Job(conf, "word count")
    job.setJarByClass(classOf[TokenizerMapper])
    job.setMapperClass(classOf[TokenizerMapper])
    job.setCombinerClass(classOf[IntSumReducer])
    job.setReducerClass(classOf[IntSumReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path((args(1))))

    if (job.waitForCompletion(true)) 0 else 1
  }
}