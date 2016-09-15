package it.polimi.genomics.scidbapi.schema

/**
  * Represents a valid SciDB data type
  */
object DataType extends Enumeration
{
  type DataType = Value

  val INT64 = Value("int64")
  val DOUBLE = Value("double")
  val BOOL = Value("bool")
  val STRING = Value("string")

  val UINT64 = Value("uint64")

  /**
    * Return the enumeration item starting from
    * the value
    *
    * @param value
    * @return
    */
  def fromValue(value:String) : DataType = value match
  {
    case "int64"  => INT64
    case "double" => DOUBLE
    case "bool"   => BOOL
    case "string" => STRING
  }
}
