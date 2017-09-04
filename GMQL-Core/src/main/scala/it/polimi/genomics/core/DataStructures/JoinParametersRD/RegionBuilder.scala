package it.polimi.genomics.core.DataStructures.JoinParametersRD


/**
 * Created by pietro on 15/06/15.
 */
object RegionBuilder extends Enumeration{
// BOTH_LEFT, keeps both regions while the left region will be in the coordinates and the right region coordinates will be as att/Val.
  // BOTH_RIGHT, keeps both regions while the right region will be in the coordinates and the left region coordinates will be as att/Val.

  type RegionBuilder = Value
  val LEFT, LEFT_DISTINCT, RIGHT, RIGHT_DISTINCT, INTERSECTION, CONTIG, BOTH = Value
}

