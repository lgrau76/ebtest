package com.earlybirds

import com.earlybirds.transforms.EarlyTransform
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{StructField, StructType, _}
import org.scalatest.{FunSuite, GivenWhenThen, Inspectors, Matchers}

class DataInitDflookupUserJoinTests extends  FunSuite with SharedSparkContext with Matchers with Inspectors with GivenWhenThen with DataFrameSuiteBase {
  lazy implicit val _spark = spark

  import _spark.implicits._

  System.setProperty("hadoop.home.dir", "C:\\_DATAS\\hadoop-winutils-2.7.0") //configure HADOOP_HOME environment variable

  val row1 = ("5e432220-8991-11e6-ac16-6945cf2d3541", "p_500315456", "1", "1476686549844")
  val row2 = ("ddfd8b10-9434-11e6-b081-b792acbf679e", "p_504922691", "1", "1476686550002")
  val row3 = ("5e432220-8991-11e6-ac16-6945cf2d3541", "p_500315456", "1", "1476686550033")
  val row4 = ("dfff0060-9434-11e6-b916-0fd2eb2adb51", "p_500315456", "1", "1476686550097")
  val data1 = Seq(row1, row2, row3, row4)

  test("verify target DataInitDflookupUserJoin") {
    Given("dataframe from EarlyTransform.getDataInit")
    val schema = StructType(
      List(
        StructField("itemIdAsInteger", IntegerType, true),
        StructField("rating", FloatType, true),
        StructField("timestamp", LongType, true),
        StructField("userIdAsInteger", IntegerType, false)
      )
    )
    val df1 = EarlyTransform.getDataInit(data1.toDF("_c0", "_c1", "_c2", "_c3"))

    val row1 = (504922691, 1.0, 1476686550, 0)
    val row2 = (500315456, 1.0, 1476686550, 1)
    val row3 = (500315456, 1.0, 1476686549, 1)
    val row4 = (500315456, 1.0, 1476686550, 2)
    val data2 = Seq(row1, row2, row3, row4)
    val dfTarget = data2.toDF("itemIdAsInteger", "rating", "timestamp", "userIdAsInteger")

    When("dfDataInitDflookupUserJoin set schema target and join")
    val dflookupUser0 = EarlyTransform.addColumnIndex(df1.select($"userIdSrc").distinct, "userIdAsInteger")
      .withColumnRenamed("userIdSrc", "userId")
    val dflookupUser = dflookupUser0.select($"userId" , expr("cast(userIdAsInteger as int) userIdAsInteger"))

    val dfSourceJoin = df1.join(dflookupUser, $"userIdSrc" === $"userId")
      .drop($"userIdSrc")
      .drop($"itemIdSrc")
      .drop($"userId")
      .drop($"itemId")

    Then("verify equals")
    dfSourceJoin.schema shouldBe schema
    dfSourceJoin.count shouldBe 4
    dfSourceJoin.collect() shouldBe dfTarget.collect()

  }
}
