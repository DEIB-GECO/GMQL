package it.polimi.genomics.core.DataStructures.MetadataCondition

/**
 * Check the conjunction of two predicates. Both predicates must be true.
 */
case class AND(first_predicate : MetadataCondition, second_predicate : MetadataCondition) extends MetadataCondition{

}
