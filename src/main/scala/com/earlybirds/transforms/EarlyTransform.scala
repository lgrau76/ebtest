package com.earlybirds.transforms

import com.earlybirds.utils.DFCommon
import org.apache.spark.sql.functions.{col, expr, max}
import org.apache.spark.sql.types.{FloatType, IntegerType}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.DataFrame

//first data source dataframe transform
object EarlyTransform extends DFCommon {

  //set schema colums names and types
  def getDataInit(dfSourceFile: DataFrame): DataFrame = {
    val dfSource = dfSourceFile
      .withColumnRenamed("_c0", "userIdSrc")
      .withColumnRenamed("_c1", "itemIdSrc")
      .withColumn("itemIdAsInteger", expr("substring(itemIdSrc,3,length(itemIdSrc)-2)").cast(IntegerType))
      .withColumn("rating", col("_c2").cast(FloatType)).drop("_c2")
      .withColumn("timestamp", expr("substring(_c3,1,length(_c3)-3)").cast(LongType)).drop("_c3")
    dfSource
  }

}