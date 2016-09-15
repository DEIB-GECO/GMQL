package it.polimi.genomics.core.DataStructures.JoinParametersRD

/**
 * Each join condition can be made of up to 4 atomic condition
 * If more than one atomic condition is provided, then the filtering must be performed in order
 */
case class JoinQuadruple(first : Option[AtomicCondition],
                         second : Option[AtomicCondition] = None,
                         third : Option[AtomicCondition] = None,
                         fourth : Option[AtomicCondition] = None) {

  def toList() = {
    List(first, second, third, fourth).flatMap(x => x)
  }

}
