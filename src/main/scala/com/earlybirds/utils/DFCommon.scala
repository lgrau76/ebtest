package com.earlybirds.utils

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.types.IntegerType

//data source dataframe get from file
trait DFCommon {

  //data source dataframe get from file read
  def readDataFromFileSrc(file_type: String, file_location: String)(implicit spark: SparkSession): DataFrame = {

    //CSV options
    val infer_schema = "false"
    val first_row_is_header = "false"
    val delimiter = ","

    //The applied options are for CSV files. For other file types, these will be ignored.
    print("spark => " + spark)

    val dfSourceFile = spark.read.format(file_type)
      .option("inferSchema", infer_schema)
      .option("header", first_row_is_header)
      .option("sep", delimiter)
      .load(file_location)
    dfSourceFile
  }

  //data target file write from dataframe
  def writeDataFromDF(df: DataFrame, numPart: Int, file_location: String)(implicit spark: SparkSession) = {
    df.coalesce(numPart).write.mode("overwrite")
      .option("header", "true")
      .csv(file_location)
  }

  //add dataframe ColumnIndex from
  def addColumnIndex(df: DataFrame, colName: String)(implicit spark: SparkSession) = {
    spark.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map {
        case (row, index) => Row.fromSeq(row.toSeq :+ index)
      },
      // Create schema for index column
      StructType(df.schema.fields :+ StructField(colName, LongType, false)))
  }

  def infoShow(df: DataFrame, dfName: String): Unit ={
    print("dfName => " + dfName)
    df.printSchema
    df.count
    df.show
  }

}

