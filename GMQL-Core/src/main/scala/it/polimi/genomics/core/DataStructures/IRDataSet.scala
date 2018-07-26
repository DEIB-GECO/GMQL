package it.polimi.genomics.core.DataStructures

import it.polimi.genomics.core.ParsingType.PARSING_TYPE

case class IRDataSet(var position:String,
                     schema:java.util.List[(String,PARSING_TYPE)]) {
  override def toString: String = position
}
