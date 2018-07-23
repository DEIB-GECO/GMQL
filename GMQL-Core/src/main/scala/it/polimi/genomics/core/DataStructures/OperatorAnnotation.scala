package it.polimi.genomics.core.DataStructures

sealed trait OperatorAnnotation {
  val annotation : String
}

case object SPLIT_POINT extends OperatorAnnotation{
  override val annotation: String = "SPLIT_POINT"
}