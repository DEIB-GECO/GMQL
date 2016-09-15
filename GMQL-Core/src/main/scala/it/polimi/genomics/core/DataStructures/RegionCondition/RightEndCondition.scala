package it.polimi.genomics.core.DataStructures.RegionCondition

import it.polimi.genomics.core.DataStructures.RegionCondition.REG_OP.REG_OP

/**
 * Check the condition on the right-end of the region.
 * @param op the operator of the condition
 * @param value the value of the condition
 */
case class RightEndCondition(op : REG_OP, value : Long) extends RegionCondition {

}