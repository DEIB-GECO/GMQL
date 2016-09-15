package it.polimi.genomics.core.DataStructures.RegionCondition

import it.polimi.genomics.core.DataStructures.RegionCondition.REG_OP.REG_OP



case class Predicate(position : Int, operator : REG_OP,  value : Any) extends RegionCondition {

}
