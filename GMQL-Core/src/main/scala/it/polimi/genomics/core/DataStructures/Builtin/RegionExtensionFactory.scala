package it.polimi.genomics.core.DataStructures.Builtin

import it.polimi.genomics.core.DataStructures.RegionAggregate.{RegionFunction, RENode, RegionExtension}

/**
  * Created by pietro on 04/05/16.
  */
trait RegionExtensionFactory {

  def get(dag : RENode, output : Either[String, Int]) : RegionFunction
}
