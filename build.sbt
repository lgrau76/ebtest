name := "earlyBirdGrauTest"

version := "1.0"

scalaVersion := "2.11.8"


val sparkVersion = "2.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  "com.holdenkarau" %% "spark-testing-base" % "2.4.0_0.11.0" % Test,
  "org.apache.spark" %% "spark-hive" % "2.4.2" % "provided"
)