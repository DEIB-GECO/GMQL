package it.polimi.genomics.core.DataStructures.GroupRDParameters


sealed trait GroupingParameter

case class FIELD(position : Int) extends GroupingParameter

