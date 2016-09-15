package it.polimi.genomics.flink.FlinkImplementation.reader.parser

import it.polimi.genomics.core.ParsingType

/**
 * It parses tab-delimited files in the format: chromosome start stop score
 */
object BedScoreParser2 extends DelimiterSeparatedValuesParser('\t',0,1,2,Some(3),Some(Array((4,ParsingType.DOUBLE)))){

  schema = List(("score", ParsingType.DOUBLE))
}
