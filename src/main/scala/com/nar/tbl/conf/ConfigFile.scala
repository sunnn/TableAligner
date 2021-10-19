package com.nar.tbl.conf

import org.json.simple.parser.JSONParser
import org.json.simple.{JSONArray, JSONObject}


class TableConf
{
  var paths : Seq[(String,String)] =null
  var transforms : Seq[(String,String,String)] = null
}


object ConfigFile {
  def getconfig(conf: String): TableConf = {
    val jsonParser = new JSONParser()
    val configJsonstr = scala.io.Source.fromFile(conf).getLines.mkString
    val taskconfig: JSONObject = jsonParser.parse(configJsonstr).asInstanceOf[JSONObject]
    println(taskconfig.getClass)
    val tblConf = new TableConf()
    val transforms: JSONArray = taskconfig.get("modification").asInstanceOf[JSONArray]
    tblConf.transforms = (0 until transforms.size()).map(transforms.get(_).asInstanceOf[JSONObject])
      .map(t => (t.get("column").asInstanceOf[String], t.get("transform").asInstanceOf[String],
        t.get("default").asInstanceOf[String]))
    val paths: JSONArray = taskconfig.get("paths").asInstanceOf[JSONArray]
    tblConf.paths = (0 until paths.size()).map(paths.get(_).asInstanceOf[JSONObject])
      .map(t => (t.get("Srctbl").asInstanceOf[String], t.get("Tgtbl").asInstanceOf[String]))
    println(paths)
    tblConf
  }
}
