package it.polimi.genomics.core.DataStructures

import it.polimi.genomics.core.Debug.OperatorDescr

sealed trait OperatorAnnotation {
  val message : String
}

case object SPLIT_POINT extends OperatorAnnotation{
  override val message: String = "SPLIT_POINT"
}

case class EXECUTED_ON(instance: GMQLInstance) extends OperatorAnnotation{
  override val message: String = "EXECUTE ON " + instance
}

case class OPERATOR(operator: OperatorDescr) extends OperatorAnnotation{
  override val message: String = "Original Operator: " + operator
}

case class SPLIT_ID(id: Int) extends OperatorAnnotation{
  override val message: String = "SPLIT ID " + id
}