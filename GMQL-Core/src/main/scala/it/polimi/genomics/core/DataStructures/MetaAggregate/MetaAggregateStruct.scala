package it.polimi.genomics.core.DataStructures.MetaAggregate

/**
 * Created by pietro on 05/05/15.
 */
trait MetaAggregateStruct extends Serializable{
  val newAttributeName : String
  val inputAttributeNames : List[String]
//  val fun : Array[Traversable[String]] => String
}

