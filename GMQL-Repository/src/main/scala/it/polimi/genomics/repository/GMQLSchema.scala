package it.polimi.genomics.repository

import it.polimi.genomics.core.ParsingType

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
                       schemaType:GMQLSchemaTypes.Value,
                       fields:List[GMQLSchemaField])

/**
  *
  *  The schema of the sample's columns descriped in the schema XML file
  *
  * @param name NAme of the field (column name)
  * @param fieldType Type of the column
  */
case class GMQLSchemaField(name:String, fieldType:ParsingType.Value)

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
/**
  * Enum for the types of schemas that we can have in our repository
  */

object GMQLSchemaTypes extends Enumeration {
  type schemaType = Value
  val GTF = Value("GTF")
  val VCF = Value("VCF")
  val Delimited = Value("DEL")
}