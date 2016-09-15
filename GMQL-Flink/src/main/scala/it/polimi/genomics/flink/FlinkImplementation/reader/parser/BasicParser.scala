package it.polimi.genomics.flink.FlinkImplementation.reader.parser

/**
 * It parses tab-delimited files in the format: chromosome start stop score
 */
object BasicParser extends DelimiterSeparatedValuesParser('\t', 0, 1, 2, None, None){

  schema = List()
}
