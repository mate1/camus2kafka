package com.mate1.camus2kafka

import org.apache.hadoop.util.{Tool, ToolRunner}

/**
 * Created with IntelliJ IDEA.
 * User: Boris Fersing
 * Date: 9/27/13
 * Time: 10:01 AM
 */

/**
 * This object is the JobRunner with the main method that gets called from the command line
 * The first argument must be the class name of the job to run. All other arguments are passed to the job.
 *
 * If the job extends the Callback trait, then the success or the error callback gets called when the job finishes
 */
object JobRunner {

  def main(args:Array[String]): Unit = {

    // The user must provide at least the class name of the job he wants to run
    args.size match {
      case 0 => println("Please provide the class name of the Job you want to run.")
      case size => {
        val classOption = try {
          Some(Class.forName(args(0)))
        } catch {
          case e: ClassNotFoundException => {
            println("The job class %s can't be found.".format(args(0)))
            None
          }
          case e => {
            e.printStackTrace()
            None
          }
        }

        classOption match {
          case Some(classz) => {

            // The first argument is the class name, don't pass it to the job
            val jobArgs = args.drop(1)

            // Get a new instance of the job
            val tool = classz.newInstance().asInstanceOf[Tool]

            // Run the job and save the exitCode for later
            val exitCode = ToolRunner.run(tool, jobArgs)

            // If the job extends the Callback trait then run the callback corresponding to the exit code
            tool match {
              case cb : C2KJob => if (exitCode == 0) cb.successCallback else cb.errorCallback
              case _ => ()
            }
          }
          case None => ()
        }
      }
    }
  }
}