package it.polimi.genomics.scidb.libraries

import it.polimi.genomics.scidbapi.expression.SciFunction
import it.polimi.genomics.scidbapi.schema.DataType._

/**
  * The object provides a set of functions from the
  * gmql4scidb library
  */
object gmql4scidb
{
  val hash = SciFunction("gmql_hash", List(STRING), INT64)
  val key_sort = SciFunction("gmql_key_sort", List(STRING), STRING)
}
