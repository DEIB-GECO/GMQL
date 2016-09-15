package it.polimi.genomics.core.DataStructures.CoverParameters

/**
 * Created by pietro on 08/05/15.
 */
sealed trait CoverParam

case class ALL() extends CoverParam
case class ANY() extends CoverParam
case class N(n : Int) extends CoverParam
