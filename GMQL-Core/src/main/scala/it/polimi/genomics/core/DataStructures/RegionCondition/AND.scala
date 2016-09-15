package it.polimi.genomics.core.DataStructures.RegionCondition

/**
 * Check the conjunction of two predicates. Both predicates must be true.
 */
case class AND(first_predicate : RegionCondition, second_predicate : RegionCondition) extends RegionCondition{

}
