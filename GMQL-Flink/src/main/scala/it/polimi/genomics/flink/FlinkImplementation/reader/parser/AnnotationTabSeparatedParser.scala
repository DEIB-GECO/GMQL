package it.polimi.genomics.flink.FlinkImplementation.reader.parser

import it.polimi.genomics.core.ParsingType

/**
 * Sample Bed3 file parser that uses tab <code>\t</code> character as delimiter
 *
 * Created by michelebertoni on 26/04/15.
 */
object AnnotationTabSeparatedParser extends DelimiterSeparatedValuesParser('\t',0,1,2,Some(5),Some(Array((3,ParsingType.DOUBLE), (4,ParsingType.DOUBLE)))){

}
