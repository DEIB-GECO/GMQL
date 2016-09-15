package it.polimi.genomics.core.DataStructures.RegionCondition

import it.polimi.genomics.core.DataStructures.RegionCondition.REG_OP.REG_OP

/**
 * Check the value of the "stop" of a region. The stop of the region corresponds
 * to the right-end if the strand if either "+" or "*", otherwise it is the region left-end.
 * @param op the operator of the condition
 * @param value the value of the condition
 */
case class StopCondition(op : REG_OP, value : Long) extends RegionCondition {

}