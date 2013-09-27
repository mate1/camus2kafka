package com.mate1.camus2kafka

/**
 * Created with IntelliJ IDEA.
 * User: Boris Fersing
 * Date: 9/27/13
 * Time: 10:22 AM
 */
trait Callback {
  def successCallback = println("Everything was OK!")
  def errorCallback = println("Something went wrong!")
}
