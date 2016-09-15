package it.polimi.genomics.core.DataStructures.MetadataCondition

/**
 * A single predicate of a MetadataCondition (e.g. MissingAttribute(antibody) )
 * This predicate is true if and only if it does not exist any metadata tuple with the given attribute name.
 * @param attribute the attribute name that has to be absent.
 */
case class ContainAttribute(attribute : String) extends MetadataCondition {

}
