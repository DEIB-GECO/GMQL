package it.polimi.genomics.core.DataStructures.MetaAggregate

trait MetaAggregateFunction extends MetaAggregateStruct{

  val fun : Array[Traversable[String]] => String
}
