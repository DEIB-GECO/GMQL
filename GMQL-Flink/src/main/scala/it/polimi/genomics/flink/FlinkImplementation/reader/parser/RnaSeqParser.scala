package it.polimi.genomics.flink.FlinkImplementation.reader.parser

import it.polimi.genomics.core.ParsingType

/**
 * It parses tab-delimited files in the format: chromosome start stop strand name score
 */
object RnaSeqParser extends DelimiterSeparatedValuesParser('\t', 0, 1, 2, Some(3), Some(Array((4, ParsingType.STRING), (5,ParsingType.DOUBLE)))){

  schema = List(("name", ParsingType.STRING), ("score", ParsingType.DOUBLE))
}
