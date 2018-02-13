package it.polimi.genomics.core.DataStructures

object OperatorType extends Enumeration {
  type OperatorType = Value

  val REGION_OPERATOR, META_OPERATOR, META_GROUP_OPERATOR, META_JOIN_OPERATOR = Value
}
