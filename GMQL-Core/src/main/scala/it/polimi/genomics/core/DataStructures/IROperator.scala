package it.polimi.genomics.core.DataStructures

/**
 * It represent a generic Intermediate Representation Daga operator
 */
class IROperator {
  val operatorName = this.getClass.getName.substring(this.getClass.getName.lastIndexOf('.')+1) + " " + this.hashCode()

  var intermediateResult : Option[AnyRef] = None

  override def toString = operatorName

  def getOperatorList : List[IROperator] = {
    var list = List[IROperator]()

    for( v <- this.getClass.getMethods) {
      if(v.getName != "getOperatorList" && v.getParameterCount == 0) {
        try{
          val value = v.invoke(this)
          value match {
            case x:IROperator =>
//              println(x)
//              println(v.getName)
              list = x :: list
          }
        }catch {
          case _ =>
        }
      }
    }
//    println(list.distinct)
    list = list.distinct
    list
  }


}

/** Indicates a IROperator which returns a metadata dataset */
class MetaOperator extends IROperator

/** Indicates a IROperator which returns a region dataset */
class RegionOperator extends IROperator {
  var binSize : Option[Long] = None
}

/** Indicates a IROperator which returns the result of a meta-group operation */
class MetaGroupOperator extends IROperator

/** Indicates a IROperator which returns the result of a meta-join operation*/
class MetaJoinOperator extends IROperator


/** Structures for specifying an Optional MetaJoinOperator **/
abstract class OptionalMetaJoinOperator(operator:MetaJoinOperator) extends Serializable
{
  def getOperator : MetaJoinOperator = operator
}
case class SomeMetaJoinOperator(operator : MetaJoinOperator) extends OptionalMetaJoinOperator(operator)
case class NoMetaJoinOperator(operator : MetaJoinOperator) extends OptionalMetaJoinOperator(operator)
