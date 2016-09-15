package it.polimi.genomics.core.DataStructures.MetadataCondition

/**
 * Operators that can be used within a MetadataCondition statement
 */
object META_OP extends Enumeration{

  type META_OP = Value
  val EQ, NOTEQ, LT, GT, LTE, GTE = Value

}
