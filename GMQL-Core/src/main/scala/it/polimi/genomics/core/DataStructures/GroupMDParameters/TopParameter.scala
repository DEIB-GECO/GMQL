package it.polimi.genomics.core.DataStructures.GroupMDParameters

/**
 * Created by pietro on 08/05/15.
 */
sealed trait TopParameter

case class NoTop() extends TopParameter
case class Top(k : Int) extends TopParameter
case class TopG(k : Int) extends TopParameter
