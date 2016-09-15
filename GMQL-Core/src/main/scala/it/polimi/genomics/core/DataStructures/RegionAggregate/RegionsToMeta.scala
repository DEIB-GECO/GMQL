package it.polimi.genomics.core.DataStructures.RegionAggregate

import it.polimi.genomics.core.GValue

/**
 * Created by michelebertoni on 08/05/15.
 */

trait R2MAggregator extends Serializable {
  var function_identifier : String = ""
  var input_index : Int = -1
  var output_attribute_name : String = ""
}
trait RegionsToMeta extends R2MAggregator {
  val newAttributeName : String
  val inputIndex : Int
  val associative : Boolean
  val fun : List[GValue] => GValue
  val funOut : (GValue,Int) => GValue
}
