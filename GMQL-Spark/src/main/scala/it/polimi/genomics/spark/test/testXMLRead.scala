package it.polimi.genomics.spark.test

import it.polimi.genomics.core.ParsingType

import scala.xml.XML

/**
  * Created by abdulrahman on 21/04/16.
  */
object testXMLRead {
  def main(args: Array[String]) {
    val XMLfile = "/Users/abdulrahman/gmql_repository/data/abdulrahman/datasets/ann.xml"

    val dsXML = XML.loadFile(XMLfile);
    val cc = (dsXML \\ "url")
    val name = (dsXML \\ "dataset").head.attribute("name").get.head.text
    println(name)
    cc.map(x => println(x.text.trim, x.attribute("id").get.head.text))


    // Loading schema
    val schemaFields = (XML.loadFile("/Users/abdulrahman/gmql_repository/data/abdulrahman/schema/ann.schema") \\ "field")
    schemaFields.map(x => println(x.text.trim, attType(x.attribute("type").get.head.text))).toList

  }

  def attType(x: String) = x.toUpperCase match {
    case "STRING" => ParsingType.STRING
    case "CHAR" => ParsingType.STRING
    case "CHARACTAR" => ParsingType.STRING
    case _ => ParsingType.DOUBLE
  }

}
