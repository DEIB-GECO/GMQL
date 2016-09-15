package it.polimi.genomics.core.DataStructures.JoinParametersRD

/**
 * Created by pietro on 15/06/15.
 */
sealed trait AtomicCondition {

}

case class DistLess(limit : Long) extends AtomicCondition {
  override def toString(): String ={
    "DistLess " + limit
  }
}
case class DistGreater(limit : Long) extends AtomicCondition {
  override def toString(): String ={
    "DistGreat " + limit
  }
}
case class MinDistance(number : Int) extends AtomicCondition {
  override def toString(): String ={
    "MinDist " + number
  }
}
case class Upstream() extends AtomicCondition {
  override def toString(): String ={
    "Upstream"
  }
}
case class DownStream() extends AtomicCondition {
  override def toString(): String ={
    "Downstream"
  }
}
