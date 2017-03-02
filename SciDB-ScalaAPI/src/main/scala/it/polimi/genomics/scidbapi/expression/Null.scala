package it.polimi.genomics.scidbapi.expression

import it.polimi.genomics.scidbapi.schema.DataType._

/**
  * This enumeration provides standard values to
  * represent null values
  */
object Null extends Enumeration
{
  type Null = Value

  val NULL_INT64 = Value("null_int64")
  val NULL_DOUBLE = Value("null_double")
  val NULL_BOOL = Value("null_bool")
  val NULL_STRING = Value("null_string")
  val NULL_BOUND = Value("null_bound")

  /**
    * This method provides the null value for a certain
    * data type
    *
    * @param datatype data type
    * @return null value
    */
  def nil(datatype: DataType) : Null = datatype match
  {
    case INT64 => NULL_INT64
    case DOUBLE => NULL_DOUBLE
    case BOOL => NULL_BOOL
    case STRING => NULL_STRING
  }
}