package it.polimi.genomics.core.DataStructures.RegionCondition

/**
 * Check the disjunction of two predicates. At least one f the two predicates must be true.
 */
case class OR(first_predicate : RegionCondition, second_predicate : RegionCondition) extends RegionCondition{

}
