package com.nar.tbl

import com.nar.tbl.util.TableUtil
import org.apache.spark.sql.SparkSession

object Controller extends App {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark Hive Test")
    .enableHiveSupport()
    .getOrCreate()
  TableUtil.apply_config(spark,"/home/mapr/tableprops.json")
}
