package it.polimi.genomics.core.DataStructures.MetadataCondition

import it.polimi.genomics.core.DataStructures.MetadataCondition.META_OP.META_OP


/**
 * A single predicate of a MetadataCondition (e.g antibody=='CTCF')
 * @param attribute_name the attribute name to be checked (e.g. "antibody")
 * @param operator the comparison operator (e.g. "EQ")
 * @param value the value (e.g. "CTCF")
 */
case class Predicate(attribute_name : String, operator : META_OP,  value : Any) extends MetadataCondition {

}
