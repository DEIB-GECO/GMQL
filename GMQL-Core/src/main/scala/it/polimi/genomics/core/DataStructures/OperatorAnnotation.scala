package it.polimi.genomics.core.DataStructures

sealed trait OperatorAnnotation {
  val message : String
}

case object SPLIT_POINT extends OperatorAnnotation{
  override val message: String = "SPLIT_POINT"
}

case class EXECUTED_ON(instance: GMQLInstance) extends OperatorAnnotation{
  override val message: String = "EXECUTE ON " + instance
}


case class SPLIT_ID(id: Int) extends OperatorAnnotation{
  override val message: String = "SPLIT ID " + id
}

case object PROTECTED extends OperatorAnnotation{
  override val message: String = "PROTECTED VARIABLE"
}