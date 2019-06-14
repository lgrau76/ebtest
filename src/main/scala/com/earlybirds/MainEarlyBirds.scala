package com.earlybirds

import com.earlybirds.utils.SparkJob

object MainEarlyBirds extends App {

  //get sparkSession
  implicit val spark = SparkJob.spark

}
