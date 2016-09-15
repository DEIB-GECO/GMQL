package it.polimi.genomics.repository.GMQLRepository

import javax.xml.bind.annotation._

import it.polimi.genomics.core.ParsingType
/**
  * Created by Abdulrahman Kaitoua on 11/04/16.
  */

/**
  *
  * @param name
  * @param Type
  * @param fields
  */

case class GMQLSchema ( name:String,
                       Type:GMQLSchemaTypes.Value,
                       fields:List[GMQLSchemaField])

/**
  *
  * @param name
  * @param fieldType
  */

case class GMQLSchemaField(name:String, fieldType:GMQLParse.Value)

object GMQLParse extends Enumeration{

  type GMQLParse = Value
  val INTEGER = Value("INTEGER")
  val DOUBLE =  Value("DOUBLE")
  val STRING =  Value("STRING")
  val NULL =  Value("NULL")
  val CHAR = Value("CHAR")
  val FLOAT = Value("FLOAT")
  val LONG = Value("LONG")
}
/**
  * Enum for the types of schemas that we can have in our repository
  */

object GMQLSchemaTypes extends Enumeration {
  type schemaType = Value
  val GTF = Value("GTF")
  val VCF = Value("VCF")
  val Delimited = Value("DEL")
}