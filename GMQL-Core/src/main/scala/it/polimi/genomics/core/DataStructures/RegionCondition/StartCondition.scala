package it.polimi.genomics.core.DataStructures.RegionCondition

import it.polimi.genomics.core.DataStructures.RegionCondition.REG_OP.REG_OP

/**
 * Check the value of the "start" of a region. The start of the region corresponds
 * to the left-end if the strand if either "+" or "*", otherwise it is the region right-end.
 * @param op the operator of the condition
 * @param value the value of the condition
 */
case class StartCondition(op : REG_OP, value : Long) extends RegionCondition {

}
