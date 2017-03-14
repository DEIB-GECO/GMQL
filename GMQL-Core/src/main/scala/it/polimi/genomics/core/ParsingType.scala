package it.polimi.genomics.core

/**
 * Created by michelebertoni on 07/05/15.
 */
object ParsingType extends Enumeration{

  type PARSING_TYPE = Value
  val INTEGER, DOUBLE, STRING, CHAR, LONG, NULL = Value


  /**
    *
    * Get the data type correspondance type in GMQL
    *
    * @param x [[ String]] of the data type
    * @return
    */
  def attType(x: String): ParsingType.Value = x.toUpperCase match {
    case "STRING" => ParsingType.STRING
    case "CHAR" => ParsingType.STRING
    case "CHARACTAR" => ParsingType.STRING
    case "LONG" => ParsingType.DOUBLE
    case "INTEGER" => ParsingType.DOUBLE
    case "INT" => ParsingType.DOUBLE
    case "BOOLEAN" => ParsingType.STRING
    case "BOOL" => ParsingType.STRING
    case _ => ParsingType.DOUBLE
  }
}