package com.earlybirds

import com.earlybirds.transforms._
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.types.{StructField, StructType, _}
import org.scalatest.{FunSuite, GivenWhenThen, Inspectors, Matchers}
import org.apache.spark.sql.functions.expr

/*
In a FunSuite, tests are function values. (The “Fun” in FunSuite stands for function.)
You denote tests with test and provide the name of the test as a string enclosed in parentheses,
followed by the code of the test in curly braces. Here's an example:
 */

/*
Using matchers
ScalaTest provides a domain specific language (DSL) for expressing assertions in tests using the word should. Just mix in
 */

/* https://github.com/holdenk/spark-testing-base/wiki/DataFrameSuiteBase   !!!!
DataFrameSuiteBase enables you to check if two DataFrames are equal. It also provides an easy way to get SparkContext and sqlContext. SparkContext and SqlContext are initialized before all testcases, So you can access them inside any test case.
For Java users the same functionality is supported by JavaDataFrameSuiteBase.
For Java users the same functionality is supported by JavaDataFrameSuiteBase.
You can assert the DataFrames equality using method assertDataFrameEquals.
 */

/* https://github.com/holdenk/spark-testing-base/wiki/SharedSparkContext !!!
Instead of initializing SparkContext before every test case or per class you can easily get your SparkContext by extending SharedSparkContext.
SharedSparkContext initializes SparkContext before all test cases and stops this context after all test cases.
For Spark 2.2 and higher you can also share the SparkContext (and SparkSession if in DataFrame tests)
between tests by adding override implicit def reuseContextIfPossible: Boolean = true to your test.
 */

class dflookupTests extends  FunSuite with SharedSparkContext with Matchers with Inspectors with GivenWhenThen with DataFrameSuiteBase {
  lazy implicit val _spark = spark
  import _spark.implicits._

  //System.setProperty("hadoop.home.dir", "C:\\_DATAS\\hadoop-winutils-2.7.0")  //configure HADOOP_HOME environment variable

  val row1 = ("5e432220-8991-11e6-ac16-6945cf2d3541","p_500315456","1","1476686549844")
  val row2 = ("ddfd8b10-9434-11e6-b081-b792acbf679e","p_504922691","1","1476686550002")
  val row3 = ("5e432220-8991-11e6-ac16-6945cf2d3541","p_500315456","1","1476686550033")
  val row4 = ("dfff0060-9434-11e6-b916-0fd2eb2adb51","p_500315456","1","1476686550097")
  val data1 = Seq(row1, row2, row3, row4)

  test("verify target dataframe DataInit") {
    Given("dataframe with colums userId,itemId,rating,timestamp")
    val schema = StructType(
      List(
        StructField("userIdSrc", StringType, true),
        StructField("itemIdSrc", StringType, true),
        StructField("itemIdAsInteger", IntegerType, true),
        StructField("rating", FloatType, true),
        StructField("timestamp", LongType, true)
      )
    )
    val df1 = data1.toDF("_c0","_c1","_c2","_c3")

    When("EarlyTransform.getDataInit")
    val df2 = EarlyTransform.getDataInit(df1)
    df1.printSchema()
    df2.printSchema()

    Then("verify schema equals")
    df2.schema shouldBe schema
  }

  test("verify target dflookupUser") {
    Given("dataframe from EarlyTransform.getDataInit")
    val schema = StructType(
      List(
        StructField("userId", StringType, true),
        StructField("userIdAsInteger", IntegerType, false)
      )
    )
    val df1 = data1.toDF("_c0","_c1","_c2","_c3")
    val dflookupUserTmp = EarlyTransform.getDataInit(df1)

    When("dflookupUser set schema target")
    val dflookupUser0 = EarlyTransform.addColumnIndex(dflookupUserTmp.select($"userIdSrc").distinct, "userIdAsInteger")
      .withColumnRenamed("userIdSrc", "userId")
    val dflookupUser = dflookupUser0.select($"userId" , expr("cast(userIdAsInteger as int) userIdAsInteger"))


    Then("verify schema equals")
    dflookupUser.schema shouldBe schema
    dflookupUser.count shouldBe 3
  }

  test("verify target dflookupProduct") {
    Given("dataframe from EarlyTransform.getDataInit")
    val schema = StructType(
      List(
        StructField("itemId", StringType, true),
        StructField("itemIdAsInteger", IntegerType, false)
      )
    )
    val df1 = data1.toDF("_c0","_c1","_c2","_c3")
    val dflookupProductTmp = EarlyTransform.getDataInit(df1)

    When("dflookupProduct set schema target")
    val dflookupProduct0 = EarlyTransform.addColumnIndex(dflookupProductTmp.select($"itemIdSrc").distinct, "itemIdAsInteger")
      .withColumnRenamed("itemIdSrc", "itemId")
    val dflookupProduct = dflookupProduct0.select($"itemId" , expr("cast(itemIdAsInteger as int) itemIdAsInteger"))


    Then("verify schema equals")
    dflookupProduct.schema shouldBe schema
    dflookupProduct.count shouldBe 2
  }
}

