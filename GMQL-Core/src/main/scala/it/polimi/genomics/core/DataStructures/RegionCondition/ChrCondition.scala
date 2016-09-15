package it.polimi.genomics.core.DataStructures.RegionCondition

/**
 * In order to be selected, the chromosome of the region has to be equal to the chr_name parameter of this predicate
 * @param chr_name: the accepted value of chromosome
 */
case class ChrCondition(chr_name : String) extends RegionCondition {

}
