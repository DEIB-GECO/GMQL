package it.polimi.genomics.pythonapi

import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.core.{GRecordKey, GValue}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
  * Created by Luca Nanni on 27/05/17.
  * Email: luca.nanni@mail.polimi.it
  */
class CollectedResult(collectedResult: (Iterator[(GRecordKey, Array[GValue])],
                                        Iterator[(Long, (String, String))],
                                        List[(String, PARSING_TYPE)])) {

  val VALUES_DELIMITER = "\t"
  val REGIONS_DELIMITER = "\n"
  val END_OF_STREAM = "$FINISH$"

  /*
  * Parsing of the result into a know structure
  * */

  val regions: Iterator[(GRecordKey, Array[GValue])] = collectedResult._1
  val metadata: Iterator[(Long, (String, String))] = collectedResult._2
  val schema: List[(String, PARSING_TYPE)] = collectedResult._3

  /**
    * Transofrms the list of regions to a unique string built like this:
    *
    * "chr\tstart\tstop\tstrand\tvalue1\tvalue2\t...\tvalueN\n
    * chr\tstart\tstop\tstrand\tvalue1\tvalue2\t...\tvalueN\n ..."
    **/
  def getRegionAsString(n: Int = 1): String = {
    if(regions.hasNext){
      {for (_ <- 1 to n; if regions.hasNext) yield regions.next() }
        .map(r => regionsToListOfString(r)
          .mkString(VALUES_DELIMITER)).mkString(REGIONS_DELIMITER)
    } else END_OF_STREAM
  }


  private def regionsToListOfString(x: (GRecordKey, Array[GValue])): List[String] = {
    val buffer = new ListBuffer[String]()
    buffer += x._1._1.toString //id
    buffer += x._1._2 //chromosome
    buffer += x._1._3.toString //start
    buffer += x._1._4.toString //stop
    buffer += x._1._5.toString //strand

    // adding the elements in the array
    buffer ++= x._2.map(y => {
      y.toString
    }).toList
    buffer.toList
  }

  def getMetadata: java.util.List[java.util.List[String]] = {
    this.metadata.map(x => {
      List(x._1.toString, x._2._1, x._2._2).asJava
    }).toList.asJava
  }

  def getSchema: java.util.List[java.util.List[String]] = {
    this.schema.map(x => {
      List(x._1, PythonManager.getStringFromParsingType(x._2)).asJava
    }).asJava
  }

  @deprecated
  def getNumberOfRegions: Int = {
    this.regions.length
  }

  @deprecated
  def getNumberOfMetadata: Int = {
    this.metadata.length
  }

}
