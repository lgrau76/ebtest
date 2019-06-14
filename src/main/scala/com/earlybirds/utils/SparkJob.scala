package com.earlybirds.utils

import org.apache.spark.sql.SparkSession

object SparkJob {

  val spark = SparkSession
    .builder()
    .appName("EarlBirdsTests")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

}
