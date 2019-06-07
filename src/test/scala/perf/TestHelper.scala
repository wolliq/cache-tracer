package com.amadeus

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}

trait TestHelper extends FunSuite with BeforeAndAfter{

  var cores: String = _
  var isLocal: String = _
  var testDatasetPath: String = _

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  lazy val spark: SparkSession = {
    SparkSession.builder()
      .master(s"local[${cores}]")
      .appName("test-spark-perf")
      .getOrCreate()
  }

  var df: DataFrame = _

  var args: Array[String] = _
}