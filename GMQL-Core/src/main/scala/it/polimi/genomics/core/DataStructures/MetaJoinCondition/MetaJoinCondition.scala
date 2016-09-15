package it.polimi.genomics.core.DataStructures.MetaJoinCondition

/**
 * Represent a metadata join condition between two datasets. The condition must be in the form:
 * left->attr1==right->attr1 AND left->attr2==right->attr2 AND ....
 * and it is stored as: List(attr1, attr2, ...)
 * @param attributes the list of the attributes name involved in the condition
 */
case class MetaJoinCondition(attributes : List[String]) {

}
