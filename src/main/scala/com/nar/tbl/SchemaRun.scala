package com.nar.tbl

import com.nar.tbl.util.AlignerUtil
import org.apache.spark.sql.SparkSession

object SchemaRun extends App {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark Hive Test")
    .enableHiveSupport()
    .getOrCreate()
  AlignerUtil.apply_config(spark,"/home/bitnami/json/tableprops.json")
}
