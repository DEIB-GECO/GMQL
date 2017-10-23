package it.polimi.genomics.core.DataStructures.Builtin

import it.polimi.genomics.core.DataStructures.MetaAggregate.{MetaAggregateFunction}

trait MetaAggregateFactory {

  def get(name : String, input:String, output : String) : MetaAggregateFunction
}
