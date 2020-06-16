package it.polimi.genomics.manager.Debug

import scala.util.Random
import scala.xml.XML

class Query(confFile: String) {

  private val xml = XML.load(confFile)
  def isComplex : Boolean = (xml \\ "conf" \\ "complex" \\ "enabled").text == "true"

  def numSelects : Array[Int] = Main.getExact(xml \\ "conf" \\ "complex" , "num_selects").map(_.toInt)
  def maxQueriesPerNum : Int = (xml \\ "conf" \\ "complex" \\ "max_queries_per_num" text).toInt

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

    if( isComplex ) {

      println("Generating a complex query.")
      println(datasets)


      val join_perc = Main.getExact(xml \\ "conf" \\ "complex", "join_perc").map(_.toFloat)
      //      val linear = Main.getExact(xml \\ "conf" \\ "complex", "linear").map(_=="true")

      val distless = Main.getTriplet(xml \\ "conf" \\ "complex" \\ "join", "distless")



      join_perc.flatMap( perc => {

        distless.map ( dist => {
          println("PERCENTAGE ==>"+perc)
          println("DISTLESS ==>"+dist)

          val query_name = "num_datasets_"+datasets.length + "_dle_" + dist

          var selects:Array[String] = datasets.zipWithIndex.map{ case (d, i) =>  s"D${i} = SELECT() ${d};"}.toArray[String]

          var maps : Array[String] = (0 to datasets.length-2).map( i => {
            if(i==0)
              s"R${i} = MAP() D${i} D${i+1};"
            else
              s"R${i} = MAP() R${i-1} D${i+1};"
          }).toArray[String]

          var joins : Array[String] = (0 to datasets.length-2).map( i => {
            if(i==0)
              s"R${i} = JOIN(DLE($dist)) D${i} D${i+1};"
            else
              s"R${i} = JOIN(DLE($dist)) R${i-1} D${i+1};"
          }).toArray[String]



          val indices_range = 0 to datasets.length-2
          val num_joins = (perc * indices_range.length).toInt
          val join_indices = Random.shuffle(indices_range).take(num_joins)

          val binary = indices_range.map(i => {
            if( join_indices.contains(i) )
              joins(i)
            else
              maps(i)
          }).toArray[String]

          val materialize : Array[String] = Array(s"MATERIALIZE R${datasets.length-2} INTO $query_name;")

          var query = (selects union binary union materialize).mkString("\n")
          //query = selects.take(1).union(Array(s"MATERIALIZE D0 INTO $query_name;")).mkString("\n")

          println(query)

          (query_name, query)

        })
      }).toList


    } else {


      operatorName match {

        case "JOIN" => {
          val distless = Main.getTriplet(xml \\ "conf" \\ "query", "distless")
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

}
