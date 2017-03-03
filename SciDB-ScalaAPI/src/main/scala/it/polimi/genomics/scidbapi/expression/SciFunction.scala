package it.polimi.genomics.scidbapi.expression

import it.polimi.genomics.scidbapi.schema.DataType.DataType


/**
  * The class provide a general representation for
  * a function
  *
  * @param name function name
  * @param input expected input
  * @param output expected output
  */
case class SciFunction(name : String,
                       input : List[DataType],
                       output : DataType)
