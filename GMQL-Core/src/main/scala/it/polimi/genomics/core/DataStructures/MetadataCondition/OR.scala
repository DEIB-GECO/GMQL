package it.polimi.genomics.core.DataStructures.MetadataCondition

/**
 * Check the disjunction of two predicates. At least one f the two predicates must be true.
 */
case class OR(first_predicate : MetadataCondition, second_predicate : MetadataCondition) extends MetadataCondition{

}
