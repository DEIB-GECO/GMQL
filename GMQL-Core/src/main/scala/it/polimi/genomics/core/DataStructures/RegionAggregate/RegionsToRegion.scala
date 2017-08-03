package it.polimi.genomics.core.DataStructures.RegionAggregate

import it.polimi.genomics.core.GValue
import it.polimi.genomics.core.ParsingType.PARSING_TYPE

/**
 * Created by pietro on 12/05/15.
 */
trait R2RAggregator extends Serializable {
  var function_identifier : String = ""
  var input_index : Int = -1
  var output_name : Option[String] = None
}
trait RegionsToRegion extends R2RAggregator{
  val resType : PARSING_TYPE
  val associative : Boolean
  val index : Int
  val fun : List[GValue] => GValue
  val funOut : (GValue,(Int, Int)) => GValue
}
