package it.polimi.genomics.manager.Debug

import scala.xml.XML

class Query(confFile: String) {

  private val xml = XML.load(confFile)
  def operatorName: String = (xml \\ "conf" \\ "query" \\ "operator").text

  def isBinary = operatorName match {
    case "JOIN" => true
    case "COVER" => false
    case "MAP" => true
  }


  def getNumQueries = operatorName match {

    case "JOIN" => Main.getTriplet(xml \\ "conf" \\ "query", "distless").length
    case "COVER" => Main.getTriplet(xml \\ "conf" \\ "query", "min_acc_cover").length
    case "MAP" => 1
  }

  // for unary operators datasets is a list of 1 datasets, for binary operators 2 datasets(0: ref, 1: exp)
  def getQueries(datasets: List[String], resultName: String): List[(String, String)] = {
    operatorName match {

      case "JOIN" => {
        val distless =  Main.getTriplet(xml \\ "conf" \\ "query", "distless")
        distless.map(d => {
          val query_name = resultName + "_dle_" + d
          (query_name, s"D1 = SELECT() ${datasets(0)}; D2 = SELECT() ${datasets(1)}; R = JOIN(DLE($d)) D1 D2; MATERIALIZE R INTO $query_name;")
        }).toList
      }
      case "MAP" => {
          val query_name = resultName
          List((query_name, s"D1 = SELECT() ${datasets(0)}; D2 = SELECT() ${datasets(1)}; R = MAP() D1 D2; MATERIALIZE R INTO $query_name;"))
      }

      case "COVER" => {
        val minAcc = Main.getTriplet(xml \\ "conf" \\ "query", "min_acc_cover")
        minAcc.map(m => {
          val query_name = resultName + "_minacc_" + m
          (query_name, s"D1 = SELECT() ${datasets(0)}; R = COVER($m,ANY) D1; MATERIALIZE R INTO $query_name;")
        }).toList
      }
    }


  }

}
