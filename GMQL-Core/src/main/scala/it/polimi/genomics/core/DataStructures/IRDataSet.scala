package it.polimi.genomics.core.DataStructures

import it.polimi.genomics.core.ParsingType.PARSING_TYPE

case class IRDataSet(var position:String,
                     schema:java.util.List[(String,PARSING_TYPE)],
                     instance: GMQLInstance = LOCAL_INSTANCE, //TODO: consider an other position
                     online: Boolean = true
                    ) {
  override def toString: String = position + "@" + instance
}

//TODO: move this to a better location
sealed trait GMQLInstance {
  def name: String
  override def toString: String = this.name
}

case class Instance(name: String) extends GMQLInstance
case object LOCAL_INSTANCE extends GMQLInstance {
  override def name: String = "LOCAL"
}
