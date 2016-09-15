package it.polimi.genomics.core.DataStructures.RegionCondition

import it.polimi.genomics.core.DataStructures.RegionCondition.REG_OP.REG_OP

/**
 * Check the condition on the left-end of the region.
 * @param op the operator of the condition
 * @param value the value of the condition
 */
case class LeftEndCondition(op : REG_OP, value : Long) extends RegionCondition {

}
