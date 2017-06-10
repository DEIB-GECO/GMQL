package it.polimi.genomics.core.DataStructures.Builtin

import it.polimi.genomics.core.DataStructures.MetaAggregate.{MENode, MetaAggregateStruct}

/**
  * Created by Olga Gorlova on 07.06.2017.
  */
trait MetaExtensionFactory {
  def get(dag : MENode, output : String) : MetaAggregateStruct
}