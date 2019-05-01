package it.polimi.genomics.core

import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import scala.collection.JavaConverters._

sealed trait GMQLLoaderBase

/**
 * Must contain both the region and the metadata parsers
 * @tparam IR region parser input data type
 * @tparam OR region parser output data type
 * @tparam IM metadata parser input data type
 * @tparam OM metadata parser output data type
 */
trait GMQLLoader[IR,OR,IM,OM] extends GMQLLoaderBase{

  //def region_parser : Any = None
  def meta_parser(input : IM) : OM
  def region_parser(input : IR) : OR

  var schema : List[(String, PARSING_TYPE)] = List.empty

  def getSchema = schema.asJava
}
