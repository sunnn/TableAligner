package com.nar.tbl

import com.nar.tbl.util.AlignerUtil
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object SchemaRun extends App {
  val spark = SparkSession
    .builder()
    .appName("Spark Hive Test")
    .enableHiveSupport()
    .getOrCreate()
  AlignerUtil.apply_config(spark,"/home/bitnami/json/tableprops.json")
}
