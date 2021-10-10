package com.nar.tbl.util

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.json.simple.parser.JSONParser
import org.json.simple.{JSONArray, JSONObject}

import scala.collection.mutable.ArrayBuffer


class TableConf
{
  var paths : Seq[(String,String)] =null
  var transforms : Seq[(String,String,String)] = null
}

object FileUtil  {

  def getconfig(conf: String): TableConf = {
    val jsonParser = new JSONParser()
    val configJsonstr = scala.io.Source.fromFile(conf).getLines.mkString
    val taskconfig: JSONObject = jsonParser.parse(configJsonstr).asInstanceOf[JSONObject]
    println(taskconfig.getClass)
    val tblConf = new TableConf()
    val transforms: JSONArray = taskconfig.get("transforms").asInstanceOf[JSONArray]
    tblConf.transforms = (0 until transforms.size()).map(transforms.get(_).asInstanceOf[JSONObject])
      .map(t => (t.get("column").asInstanceOf[String], t.get("transform").asInstanceOf[String],
        t.get("default").asInstanceOf[String]))
    val paths: JSONArray = taskconfig.get("paths").asInstanceOf[JSONArray]
    tblConf.paths = (0 until paths.size()).map(paths.get(_).asInstanceOf[JSONObject])
      .map(t => (t.get("Srctbl").asInstanceOf[String], t.get("Tgtbl").asInstanceOf[String]))
    println(paths)
    tblConf
  }
  def addCols(df: DataFrame, columns: Set[(String,String)]): DataFrame = {
    columns.foldLeft(df)((acc, col) => {
      acc.withColumn(col._1, functions.lit(col._2)) })
  }
  def ReplaceCols(df: DataFrame, columns: Set[(String,String)]): DataFrame = {
    columns.foldLeft(df)((acc, col) => {
      acc.withColumnRenamed(col._1, col._2) })
  }

def SetIntersect(Sset: Set[(String)],jMap:Map[String, (String, String)])={
  val deltaSet = (Sset.toSet intersect jMap.keySet) collect(jMap)
  deltaSet
}

  def constructMap(coltrans:Seq[(String, String, String)])={
    val config = coltrans.view.map{case (k,v1,v2) if v2 != "" => (k,(v1,v2))
    case (k,v1,"")  => (k,(k,v1))
    }.toMap
    config
  }


  def matchcols(ColSrc: List[(String, String)],coltrans:Seq[(String, String, String)])
  :(Set[(String,String)],Set[(String,String)])
  = {
    var klist  = ArrayBuffer[String]()
    var vlist = ArrayBuffer[String]()

    ColSrc.foreach {
      case (null,x)  =>  klist.append(x)
      case (x,null)  =>  vlist.append(x)
    }
    val SrcSet = SetIntersect(klist.toSet,constructMap(coltrans))
    val TgtSet = SetIntersect(vlist.toSet,constructMap(coltrans))

    return (SrcSet,TgtSet)
  }


  def apply_config(spark:SparkSession,cfgstr:String): Unit= {
    val config = getconfig (cfgstr)
    val coltranforms = config.transforms
    import spark.implicits._
    val SourceDf = spark.read.parquet(s"${config.paths(0).productElement(0)}")
    val SrcCols = SourceDf.columns.toList.toDF("src_col")
    val TargetDf = spark.read.parquet(s"${config.paths(0).productElement(1)}")
    val TgtCols = TargetDf.columns.toList.toDF("tgt_col")

    val SrcTgtDf = SrcCols.join(TgtCols,SrcCols("src_col")===TgtCols("tgt_col"),"outer")
      .filter(SrcCols("src_col").isNull || TgtCols("tgt_col").isNull)
      .select($"src_col", $"tgt_col").as[(String, String)].collect.toList

    println(s"This is the logic for Aligner ${SrcTgtDf},${coltranforms}")


    val (klist,vlist) = matchcols(SrcTgtDf,coltranforms)

    val RenameDf = ReplaceCols(SourceDf,vlist)

    RenameDf.show(false)
    val finaldf = addCols(RenameDf,klist)

    finaldf.show(false)

  }



}