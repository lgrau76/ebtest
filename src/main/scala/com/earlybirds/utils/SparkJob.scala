package com.earlybirds.utils

import org.apache.spark.sql.SparkSession

//create spark session (default params appName and master local[*] / with spark submit choose another mode)
object SparkJob {

  val spark = SparkSession
    .builder()
    .appName("EarlBirdsTests")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

}
