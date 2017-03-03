package it.polimi.genomics.scidb.utility

import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.core.ParsingType._
import it.polimi.genomics.scidbapi.schema.{DataType, Attribute}
import it.polimi.genomics.scidbapi.schema.DataType._

/**
  * This class provides some utilities to convert schema
  * columns into SciDB properties
  */
object SchemaUtils
{

  /**
    * Converts a schema column into a SciDB attribute
    * converting the data type
    *
    * @param name
    * @param gmqltype
    * @return
    */
  def toAttribute(name:String, gmqltype:PARSING_TYPE) : Attribute =
  {
    val scidbtype = gmqltype match
    {
      case ParsingType.INTEGER => DataType.INT64
      case ParsingType.DOUBLE => DataType.DOUBLE
      case ParsingType.STRING => DataType.STRING
    }

    Attribute(name.replace(".","$"), scidbtype)
  }

  /**
    * Converts a SciDB type to a Gmql type
    *
    * @param scitype scidb type
    * @return gmql type
    */
  def toGmqlType(scitype:DataType) : PARSING_TYPE = scitype match
  {
    case DataType.INT64 => ParsingType.INTEGER
    case DataType.DOUBLE => ParsingType.DOUBLE
    case DataType.STRING => ParsingType.STRING
  }

}
