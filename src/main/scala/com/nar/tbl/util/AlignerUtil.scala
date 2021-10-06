package com.nar.tbl.util

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.json.simple.parser.JSONParser
import org.json.simple.{JSONArray, JSONObject}


class SchemaConf
{
  var paths : Seq[(String,String)] =null
  var transforms : Seq[(String,String,String)] = null
}

object AlignerUtil  {

  def getconfig(conf: String): SchemaConf = {
    val jsonParser = new JSONParser()
    val configJsonstr = scala.io.Source.fromFile(conf).getLines.mkString
    val taskconfig: JSONObject = jsonParser.parse(configJsonstr).asInstanceOf[JSONObject]
    println(taskconfig.getClass)
    val schemaAl = new SchemaConf()
    val transforms: JSONArray = taskconfig.get("transforms").asInstanceOf[JSONArray]
    schemaAl.transforms = (0 until transforms.size()).map(transforms.get(_).asInstanceOf[JSONObject])
      .map(t => (t.get("column").asInstanceOf[String], t.get("transform").asInstanceOf[String],
        t.get("default").asInstanceOf[String]))
    val paths: JSONArray = taskconfig.get("paths").asInstanceOf[JSONArray]
    schemaAl.paths = (0 until paths.size()).map(paths.get(_).asInstanceOf[JSONObject])
      .map(t => (t.get("Srctbl").asInstanceOf[String], t.get("Tgtbl").asInstanceOf[String]))
    println(paths)
    schemaAl
  }
  def addCols(df: DataFrame, columns: Seq[String]): DataFrame = {
    columns.foldLeft(df)((acc, col) => {
      acc.withColumn(col, functions.lit(col)) })
  }
  def ReplaceCols(df: DataFrame, columns: Seq[String]): DataFrame = {
    columns.foldLeft(df)((acc, col) => {
      acc.withColumnRenamed(col, col) })
  }
  /*def whenkeyisnull(col_fil:Map[String,String]):Map[String,String]= {
    val withNulls = col_fil.filter{case (k,v)=> k == null}
    println(s"This is key null $withNulls")
    withNulls
  }
  def whenvalisnull(col_fil:Map[String,String]):Map[String,String]= {
    val withNulls = col_fil.filter{case (k,v)=> v == null}
    println(s"This is value null $withNulls")
    withNulls
  }
  def matchcolumns(col_fil :Set[String],conft:Map[String,String]):Set[String]= {
    val confval = conft.keySet
    val aligner_set = col_fil.intersect(confval)
    val collection_columns = aligner_set collect conft
    collection_columns
  }*/

  def apply_config(spark:SparkSession,cfgstr:String): Unit= {
    val config = getconfig (cfgstr)
    val coltranforms = config.transforms
    import spark.implicits._
    val SourceDf = spark.read.parquet(s"${config.paths(0).productElement(0)}")
    val SrcCols = SourceDf.columns.toList.toDF("src_col")
    val TargetDf = spark.read.parquet(s"${config.paths(0).productElement(1)}")
    val TgtCols = TargetDf.columns.toList.toDF("tgt_col")

    val SrcTgtDf = SrcCols.join(TgtCols,SrcCols("src_col")===TgtCols("tgt_col"),"outer")
    val SrcTgtNull = SrcTgtDf.filter(SrcTgtDf("src_col").isNull || SrcTgtDf("tgt_col").isNull)

    val ColSrc = SrcTgtNull.select($"src_col", $"tgt_col").as[(String, String)].collect.toMap
    println(s"This is the logic for Aligner ${ColSrc},${coltranforms}")

    /*val keyNull = whenkeyisnull(col_src)
    val valueNull = whenvalisnull(col_src)
    println(keyNull,valueNull)
    val keyNull_set = matchcolumns(keyNull.values.toSet,confColumnReplace)
    val valNull_set = matchcolumns(valueNull.keySet,confColumnReplace)
    println(keyNull_set, valNull_set)
    val srcdf_c = ReplaceCols(srcfile_df,valNull_set.toSeq)
    val finaldf_c = addCols(srcdf_c,keyNull_set.toSeq)
    finaldf_c.show(false)
    /*
    val aligner_set = col_src.keySet.intersect(confColumnReplace.keySet)
    val collection_columns = aligner_set collect confColumnReplace
    println(aligner_set,collection_columns)
    srcdf_c.show(false)*/*/
  }



}