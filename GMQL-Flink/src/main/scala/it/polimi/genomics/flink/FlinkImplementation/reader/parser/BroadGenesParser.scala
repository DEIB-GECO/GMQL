package it.polimi.genomics.flink.FlinkImplementation.reader.parser

import it.polimi.genomics.core.ParsingType

/**
 * It parses tab-delimited files in the format: chromosome start stop score
 */
object BroadGenesParser extends DelimiterSeparatedValuesParser('\t', 0, 1, 2, None, Some(Array((3,ParsingType.DOUBLE), (4,ParsingType.DOUBLE),(5,ParsingType.DOUBLE),(6,ParsingType.STRING)))){

  schema = List(("Score", ParsingType.DOUBLE),("Active", ParsingType.DOUBLE),("NotActive", ParsingType.DOUBLE),("name", ParsingType.DOUBLE))
}
