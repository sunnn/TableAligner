package com.nar.tbl.util

import org.apache.spark.sql.{DataFrame, functions}

import scala.collection.mutable.ArrayBuffer

object Functions {
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

}
