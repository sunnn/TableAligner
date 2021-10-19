package com.nar.tbl.util

import com.nar.tbl.conf.ConfigFile
import com.nar.tbl.util.Functions.{ReplaceCols, addCols}
import org.apache.spark.sql.SparkSession
import org.json.simple.parser.JSONParser





object TableUtil  {

  def apply_config(spark:SparkSession,cfgstr:String): Unit= {
    val config = ConfigFile.getconfig (cfgstr)
    import spark.implicits._
    val SourceDf = spark.read.parquet(s"${config.paths(0).productElement(0)}")
    val SrcCols = SourceDf.columns.toList.toDF("src_col")
    val TargetDf = spark.read.parquet(s"${config.paths(0).productElement(1)}")
    val TgtCols = TargetDf.columns.toList.toDF("tgt_col")

    val SrcTgt = SrcCols.join(TgtCols,SrcCols("src_col")===TgtCols("tgt_col"),"outer")
      .filter(SrcCols("src_col").isNull || TgtCols("tgt_col").isNull)
      .select($"src_col", $"tgt_col").as[(String, String)].collect.toList

    //println(s"This is the logic for Aligner ${SrcTgt},${coltranforms}")


    val (klist,vlist) = Functions.matchcols(SrcTgt,config.transforms)

    val RenameDf = ReplaceCols(SourceDf,vlist)

    RenameDf.show(false)
    val finaldf = addCols(RenameDf,klist)

    finaldf.show(false)

  }



}