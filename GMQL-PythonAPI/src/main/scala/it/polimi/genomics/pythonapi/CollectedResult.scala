package it.polimi.genomics.pythonapi

import it.polimi.genomics.core.{GRecordKey, GValue, ParsingType}
import it.polimi.genomics.core.ParsingType.PARSING_TYPE

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
  * Created by Luca Nanni on 27/05/17.
  * Email: luca.nanni@mail.polimi.it
  */
class CollectedResult( var collectedResult: Any ) {

  val VALUES_DELIMITER = "\t"
  val REGIONS_DELIMITER = "\n"

  /*
  * Parsing of the result into a know structure
  * */

  var result : (Array[(GRecordKey, Array[GValue])],
    Array[(Long, (String, String))], List[(String, PARSING_TYPE)]) =
    collectedResult.asInstanceOf[(Array[(GRecordKey, Array[GValue])],
    Array[(Long, (String, String))], List[(String, PARSING_TYPE)])]

  var regions : Array[(GRecordKey, Array[GValue])] = result._1
  var metadata : Array[(Long, (String, String))] = result._2
  var schema : List[(String, PARSING_TYPE)] = result._3

  result = null
  collectedResult = null

  /**
    * Transofrms the list of regions to a unique string built like this:
    *
    * "chr\tstart\tstop\tstrand\tvalue1\tvalue2\t...\tvalueN\n
    *  chr\tstart\tstop\tstrand\tvalue1\tvalue2\t...\tvalueN\n ..."
    * */
  def getRegionsAsString(n: Int): String = {
    var partOfRegions : Array[(GRecordKey, Array[GValue])] = regions
    if (n > 0) {
      partOfRegions = regions.take(n)
    }

    val result : String = partOfRegions.map(
      x => {
        val listOfString = this.regionsToListOfString(x)
        listOfString.mkString(VALUES_DELIMITER)
      }
    ).mkString(REGIONS_DELIMITER)
    this.regions = this.regions.drop(n)
    result
  }

  private def regionsToListOfString(x : (GRecordKey, Array[GValue])): List[String] = {
    val buffer = new ListBuffer[String]()
    buffer += x._1._1.toString  //id
    buffer += x._1._2           //chromosome
    buffer += x._1._3.toString  //start
    buffer += x._1._4.toString  //stop
    buffer += x._1._5.toString  //strand

    // adding the elements in the array
    buffer ++= x._2.map( y => {
      y.toString
    }).toList
    buffer.toList
  }

  def getRegionsAsJavaLists(n: Int): java.util.List[java.util.List[String]] = {
    var partOfRegions : Array[(GRecordKey, Array[GValue])] = regions
    if (n > 0) {
      partOfRegions = regions.take(n)
    }

    val result : Array[java.util.List[String]] = partOfRegions.map(
      x => {
        val listOfString = this.regionsToListOfString(x)
        listOfString.asJava
      }
    )
    this.regions = this.regions.drop(n)
    result.toList.asJava
  }

  def getMetadata : java.util.List[java.util.List[String]] = {
    this.metadata.map( x => {
      List(x._1.toString, x._2._1, x._2._2).asJava
    }).toList.asJava
  }

  def getSchema : java.util.List[java.util.List[String]] = {
    this.schema.map(x => {
      List(x._1, PythonManager.getStringFromParsingType(x._2)).asJava
    }).asJava
  }

  def getNumberOfRegions : Int = {
    this.regions.length
  }

  def getNumberOfMetadata : Int = {
    this.metadata.length
  }

}
