package it.polimi.genomics.flink.FlinkImplementation.reader.parser

import it.polimi.genomics.core.ParsingType

/**
  * Created by abdulrahman on 02/12/15.
  */
object ANNParser extends DelimiterSeparatedValuesParser('\t',0,1,2,Some(5),Some(Array((3,ParsingType.STRING), (4,ParsingType.DOUBLE)))){
  schema = List(("name", ParsingType.STRING),("score", ParsingType.DOUBLE))
}