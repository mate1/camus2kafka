package com.mate1.camus2kafka

import org.apache.hadoop.util.ToolRunner

/**
 * Created with IntelliJ IDEA.
 * User: Boris Fersing
 * Date: 9/27/13
 * Time: 10:01 AM
 */

/**
 * This object is the JobRunner with the main method that gets called from the command line
 */
object JobRunner {
  def main(args:Array[String]): Unit = ToolRunner.run(new Camus2KafkaJob(), args)
}