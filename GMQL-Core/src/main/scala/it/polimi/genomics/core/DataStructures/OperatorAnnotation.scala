package it.polimi.genomics.core.DataStructures

sealed trait OperatorAnnotation {
  val message : String
}

case object SPLIT_POINT extends OperatorAnnotation{
  override val message: String = "SPLIT_POINT"
}