package it.polimi.genomics.core.DataStructures.MetaJoinCondition

/**
  * generic trait for [[MetaJoinCondition]] attributes
  */
sealed trait AttributeEvaluationStrategy

/**
  * Default evaluation, two attributes match if both end
  * with [[attribute]] .
  *
  * @param attribute the attribute name [prefix.]* name
  */
case class Default(attribute : String) extends AttributeEvaluationStrategy {
  override def toString() : String = attribute
}

/**
  * Only attributes exactly as [[attribute]] will match;
  * no further prefixes are allowed.
  *
  * @param attribute the attribute name [prefix.]* name
  */
case class Exact(attribute : String) extends AttributeEvaluationStrategy {
  override def toString() : String = "Exact ( " + attribute + " ) "
}

/**
  * Two attributes match if they both end with [[attribute]]
  * and, if they have a further prefixes, the two prefix
  * sequence are identical
  *
  * @param attribute the attribute name [prefix.]* name
  */
case class FullName(attribute : String) extends AttributeEvaluationStrategy{
  override def toString() : String = "FullName ( " + attribute + " ) "
}

/**
 * Represent a metadata join condition between two datasets. The condition must be in the form:
 * left->attr1==right->attr1 AND left->attr2==right->attr2 AND ....
 * and it is stored as: List(attr1, attr2, ...)
 * @param attributes the list of the attributes name involved in the condition
 */
case class MetaJoinCondition(attributes : List[AttributeEvaluationStrategy]) {

}
