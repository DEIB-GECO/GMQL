package it.polimi.genomics.scidb.utility

import it.polimi.genomics.core.DataStructures.JoinParametersRD.{MinDistance, DistLess, JoinQuadruple, AtomicCondition}

// -------------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------------

case class JoinPredSteps(first : List[AtomicCondition],
                         second : List[AtomicCondition],
                         third : List[AtomicCondition])
{
  def toList() = List(first, second, third).flatMap(x => x)
}

// -------------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------------

object JoinPredUtils
{

  /**
    * Convert a list of quadruples into a list of step plans
    *
    * @param quadruples
    * @return
    */
  def convert(quadruples : List[JoinQuadruple]) : List[JoinPredSteps] =
    quadruples.map(q => optimize(q))

  /**
    * Optimize a single quadruple generating a three steps plan
    *
    * @param quadruple original list of predicates
    * @return the steps plan
    */
  def optimize(quadruple : JoinQuadruple) : JoinPredSteps =
  {
    var mdfound : Boolean = false

    var first : List[AtomicCondition] = List()
    var second : List[AtomicCondition] = List()
    var third : List[AtomicCondition] = List()

    for( atom <- quadruple.toList() )
      (atom, mdfound) match
      {
        case (a:DistLess, _) => first = first ::: List(a)

        case (a:MinDistance, false) => { second = List(a); mdfound = true }
        case (a:MinDistance, true) => throw new InternalError("Two MD predicates found")

        case (a, false) => first = first ::: List(a)
        case (a, true) => third = third ::: List(a)
      }

    JoinPredSteps(first, second, third)
  }

}
