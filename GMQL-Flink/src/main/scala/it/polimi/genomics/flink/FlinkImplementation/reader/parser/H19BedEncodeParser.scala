package it.polimi.genomics.flink.FlinkImplementation.reader.parser

import it.polimi.genomics.core.ParsingType

/**
 * It parses tab-delimited files in the format: chromosome start stop signal
 */
object H19BedEncodeParser extends DelimiterSeparatedValuesParser('\t', 0, 1, 2, None, Some(Array((3,ParsingType.STRING)))){

  schema = List(("signal", ParsingType.DOUBLE))
}
