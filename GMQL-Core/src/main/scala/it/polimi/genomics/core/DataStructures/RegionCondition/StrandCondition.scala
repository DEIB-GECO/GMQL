package it.polimi.genomics.core.DataStructures.RegionCondition

/**
 * In order to be selected, the strand of the region has to be equal to the strand_name parameter of this predicate
 * @param strand_name: the accepted value of strand
 */
case class StrandCondition(strand : String) extends RegionCondition {


}