package it.polimi.genomics.core.DataStructures.MetaAggregate

trait MetaAggregateFunction  extends Serializable{
  val newAttributeName: String
  val inputName: String
  val fun : Array[Traversable[String]] => String
}
