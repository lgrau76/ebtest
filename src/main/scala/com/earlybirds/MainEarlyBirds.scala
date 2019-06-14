package com.earlybirds

import com.earlybirds.transforms.EarlyTransform
import com.earlybirds.utils.SparkJob
import org.apache.spark.sql.functions.expr

/*
main
only 1 pipeline in main test project / should be encapsulated in extern object
here only 1 file by batch process
*/
object MainEarlyBirds extends App {

  //get sparkSession
  implicit val spark = SparkJob.spark

  //File location and type
  //example program args local relative data dir params : data/early/ xag.csv csv data/early/ lookupuser.csv data/early/out/lookuproduct.csv data/early/out/aggratings.csv
  val pathIn    =  args(0) //data/early/
  val fileIn    =  args(1) //xag.csv
  val fileType  =  args(2) //csv
  val pathOut   =  args(3) //data/early/
  val fileOut1  =  args(4) //lookupuser.csv
  val fileOut2  =  args(5) //data/early/out/lookuproduct.csv
  val fileOut3  =  args(6) //data/early/out/aggratings.csv

  //get dfSource from data file
  val dfSource = EarlyTransform.getDataInit(EarlyTransform.readDataFromFileSrc(fileType, pathIn+fileIn))
  //EarlyTransform.infoShow(dfSource, "dfSource")

  //get df userId,userIdAsInteger
  import spark.implicits._
  val dflookupUser0 = EarlyTransform.addColumnIndex(dfSource.select($"userIdSrc").distinct, "userIdAsInteger")
    .withColumnRenamed("userIdSrc", "userId")
    .repartition($"userId")
  val dflookupUser = dflookupUser0.select($"userId" , expr("cast(userIdAsInteger as int) userIdAsInteger"))
  EarlyTransform.infoShow(dflookupUser, "dflookupUser")

  //df itemId,itemIdAsInteger
  val dflookupProduct0 = EarlyTransform.addColumnIndex(dfSource.select($"itemIdSrc").distinct, "itemIdAsInteger")
    .withColumnRenamed("itemIdSrc", "itemId")
    .repartition($"itemId")
  val dflookupProduct = dflookupProduct0.select($"itemId" , expr("cast(itemIdAsInteger as int) itemIdAsInteger"))
  EarlyTransform.infoShow(dflookupProduct, "dflookupProduct")

  //get dfSource with userId join
  val dfSourceJoin = dfSource.repartition($"userIdSrc").join(dflookupUser, $"userIdSrc" === $"userId")
    .drop($"userIdSrc")
    .drop($"itemIdSrc")
    .drop($"userId")
    .drop($"itemId")
    .cache()
  EarlyTransform.infoShow(dfSourceJoin, "dfSourceJoin")

  //get df userIdAsInteger,itemIdAsInteger,ratingSum aggregated
  import org.apache.spark.sql.functions._
  val dfAggratingsTmp = dfSourceJoin.select($"userIdAsInteger", $"itemIdAsInteger", $"rating", $"timestamp")
    .groupBy($"userIdAsInteger", $"itemIdAsInteger", $"rating", $"timestamp")
    .agg(sum($"rating") as "ratingSumTmp")
    .cache()
  EarlyTransform.infoShow(dfAggratingsTmp, "dfAggratingsTmp")

  //get and broadcast max timestamp
  val timestampMaxBroadcast =  spark.sparkContext.broadcast(dfSourceJoin.select($"timestamp").agg(max($"timestamp") as "timestampMax").head().getLong(0))

  //get df aggregated ratingSum applied 0.95 x year and filter < 0.01
  val ratingAdjustCoeffudf = udf((rating: Double, tsPast: Long) => (rating * scala.math.pow(0.95, ((timestampMaxBroadcast.value - tsPast)/(24D * 3600D)))))
  //spark.udf.register("ratingAdjustCoeffudf", ratingAdjustCoeffudf)
  val dfAggratings  =dfAggratingsTmp.select($"userIdAsInteger", $"itemIdAsInteger", $"ratingSumTmp", $"timestamp", ratingAdjustCoeffudf($"ratingSumTmp", $"timestamp") as "ratingSum")
    .filter($"ratingSum" > 0.01)
    .select($"userIdAsInteger", $"itemIdAsInteger", $"ratingSum")
    .cache()
  EarlyTransform.infoShow(dfAggratings, "dfAggratings")

  //write df lookupuser.csv
  EarlyTransform.writeDataFromDF(dflookupUser.limit(100), 1, pathOut+fileOut1) //limit 100 for local process

  //write df lookuproduct.csv
  EarlyTransform.writeDataFromDF(dflookupProduct.limit(100), 1, pathOut+fileOut2) //limit 100 for local process

  //write df aggratings.csv
  EarlyTransform.writeDataFromDF(dfAggratings.limit(100), 1,pathOut+fileOut3) //limit 100 for local process
}
