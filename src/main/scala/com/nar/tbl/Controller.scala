package com.nar.tbl

import com.nar.tbl.util.FileUtil
import org.apache.spark.sql.SparkSession

object Controller extends App {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark Hive Test")
    .enableHiveSupport()
    .getOrCreate()
  FileUtil.apply_config(spark,"/home/bitnami/json/tableprops.json")
}
