package it.polimi.genomics.core

import it.polimi.genomics.core.ParsingType.PARSING_TYPE

/**
  * Created by Abdulrahman Kaitoua on 11/04/16.
  */

/**
  *
  * @param name
  * @param schemaType
  * @param fields
  */

case class GMQLSchema (name:String,
                       schemaType:GMQLSchemaFormat.Value,
                       schemaCoordinateSystem: GMQLSchemaCoordinateSystem.Value,
                       fields:List[GMQLSchemaField])

/**
  *
  *  The schema of the sample's columns descriped in the schema XML file
  *
  * @param name NAme of the field (column name)
  * @param fieldType Type of the column
  */
case class GMQLSchemaField(name:String, fieldType:ParsingType.Value)

/**
  * Enum for the coordinate system that is used for dataset
  */
object GMQLSchemaCoordinateSystem extends Enumeration{
  type outputFormat = Value
  val ZeroBased = Value("0-based")
  val OneBased = Value("1-based")
  val Default = Value("default")
  /**
    *
    *  Get the [[ GMQLSchemaFormat]] of a specific String
    *
    * @param coordinateSystem String of the coordinate system
    * @return
    */
  def getType(coordinateSystem:String): GMQLSchemaCoordinateSystem.Value ={
    coordinateSystem. toLowerCase() match {
      case "0-based" => GMQLSchemaCoordinateSystem.ZeroBased
      case "1-based" => GMQLSchemaCoordinateSystem.OneBased
      case _ => GMQLSchemaCoordinateSystem.Default
    }
  }
}

/**
  * Enum for the types of schemas that we can have in our repository
  */
object GMQLSchemaFormat extends Enumeration{
  type outputFormat = Value
  val TAB = Value("tab")
  val GTF = Value("gtf")
  val VCF = Value("vcf")
  val COLLECT = Value("collect")

  /**
    *
    *  Get the [[ GMQLSchemaFormat]] of a specific String
    *
    * @param schemaType String of the schema type
    * @return
    */
  def getType(schemaType:String): GMQLSchemaFormat.Value ={
    schemaType. toLowerCase() match {
      case "gtf" => GMQLSchemaFormat.GTF
      case "del" => GMQLSchemaFormat.TAB
      case "vcf" => GMQLSchemaFormat.VCF
      case _ => GMQLSchemaFormat.TAB
    }
  }
}

object GMQLSchema {
  def generateSchemaXML(schema : List[(String, PARSING_TYPE)], dsname:String,outputFormat: GMQLSchemaFormat.Value, outputCoordinateSystem: GMQLSchemaCoordinateSystem.Value): String ={
    val schemaPart = if(outputFormat == GMQLSchemaFormat.GTF) {
      "\t<gmqlSchema type=\"gtf\"" + " coordinate_system=\"" +outputCoordinateSystem.toString+"\">\n"+
        "\t\t<field type=\"STRING\">seqname</field>\n" +
        "\t\t<field type=\"STRING\">source</field>\n"+
        "\t\t<field type=\"STRING\">feature</field>\n"+
        "\t\t<field type=\"LONG\">start</field>\n"+
        "\t\t<field type=\"LONG\">end</field>\n"+
        "\t\t<field type=\"DOUBLE\">score</field>\n"+
        "\t\t<field type=\"CHAR\">strand</field>\n"+
        "\t\t<field type=\"STRING\">frame</field>"

    }else {
      "\t<gmqlSchema type=\"Peak\"" + " coordinate_system=\"" +outputCoordinateSystem.toString+"\">\n" +
        "\t\t<field type=\"STRING\">chr</field>\n" +
        "\t\t<field type=\"LONG\">left</field>\n" +
        "\t\t<field type=\"LONG\">right</field>\n" +
        "\t\t<field type=\"CHAR\">strand</field>"
    }

    val gtfFixFields = Array[String]("score","feature","source","frame")

    val schemaHeader =
      "<?xml version='1.0' encoding='UTF-8'?>\n" +
        "<gmqlSchemaCollection name=\""+dsname+"\" xmlns=\"http://genomic.elet.polimi.it/entities\">\n" +
        schemaPart +"\n"+
        schema.flatMap{x =>
          if(outputFormat == GMQLSchemaFormat.GTF && (gtfFixFields.filter(s=>x._1.toLowerCase() == s).size > 0) ) None
          else Some("\t\t<field type=\"" + x._2.toString + "\">" + x._1 + "</field>")
        }.mkString("\n") +
        "\n\t</gmqlSchema>\n" +
        "</gmqlSchemaCollection>"

    schemaHeader
  }
}

///**
//  *
//  * Data Types allowed in the xml schema file
//  *
//  */
//object GMQLParse extends Enumeration{
//
//  type GMQLParse = Value
//  val INTEGER = Value("INTEGER")
//  val DOUBLE =  Value("DOUBLE")
//  val STRING =  Value("STRING")
//  val NULL =  Value("NULL")
//  val CHAR = Value("CHAR")
//  val FLOAT = Value("FLOAT")
//  val LONG = Value("LONG")
//}

//object GMQLSchemaFormat extends Enumeration {
//  type schemaType = Value
//  val GTF = Value("GTF")
//  val VCF = Value("VCF")
//  val Delimited = Value("DEL")
//}