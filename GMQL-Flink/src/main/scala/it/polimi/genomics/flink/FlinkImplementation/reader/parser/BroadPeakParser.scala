package it.polimi.genomics.flink.FlinkImplementation.reader.parser

import it.polimi.genomics.core.ParsingType

/**
 * Created by pietro on 09/10/15.
 */
object BroadPeakParser extends DelimiterSeparatedValuesParser(
  '\t', 0, 1, 2,Some(5),
  Some(Array((3, ParsingType.STRING), (4, ParsingType.DOUBLE), (6, ParsingType.DOUBLE),
    (7, ParsingType.DOUBLE), (8, ParsingType.DOUBLE)))){

  schema = List(("name", ParsingType.STRING),
    ("score", ParsingType.DOUBLE),
    ("signalValue", ParsingType.DOUBLE),
    ("pValue", ParsingType.DOUBLE),
    ("qValue", ParsingType.DOUBLE))
}