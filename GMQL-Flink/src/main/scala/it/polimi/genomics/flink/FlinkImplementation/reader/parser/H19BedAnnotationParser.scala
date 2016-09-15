package it.polimi.genomics.flink.FlinkImplementation.reader.parser

import it.polimi.genomics.core.ParsingType

/**
 * It parses tab-delimited files in the format: chromosome start stop name score strand
 */
object H19BedAnnotationParser extends DelimiterSeparatedValuesParser('\t', 0, 1, 2, Some(5), Some(Array((3, ParsingType.STRING), (4, ParsingType.DOUBLE)))){

  schema = List(("name", ParsingType.DOUBLE), ("score", ParsingType.DOUBLE))
}
