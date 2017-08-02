package it.polimi.genomics.core.DataStructures.MetaAggregate

/**
  * Created by Olga Gorlova on 07.06.2017.
  */
trait MetaExtension extends MetaAggregateStruct {
  val fun : Array[Traversable[String]] => String
}

trait MENode extends Serializable
case class MENegate(o1:MENode) extends MENode {override def toString() = "negate(" + o1 +")"}
case class MESQRT(o1:MENode) extends MENode {override def toString() = "sqrt(" + o1 +")"}
case class MEADD(o1:MENode, o2:MENode)extends MENode {override def toString() = "add(" + o1 + "," + o2 +")"}
case class MESUB(o1:MENode, o2:MENode) extends MENode{override def toString() = "sub(" + o1 + "," + o2 +")"}
case class MEMUL(o1:MENode, o2:MENode) extends MENode{override def toString() = "mul(" + o1 + "," + o2 +")"}
case class MEDIV(o1:MENode, o2:MENode) extends MENode{override def toString() = "div(" + o1 + "," + o2 +")"}
case class MEName(name : String) extends MENode {override def toString() = "name" + name}
case class MEFloat(const : Double) extends MENode{override def toString() = "float" + const}
case class MEStringConstant(const : String) extends MENode{override def toString() = "string = " + const}
