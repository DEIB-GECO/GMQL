package it.polimi.genomics.core.DataStructures.CoverParameters

/**
 * Created by pietro on 08/05/15.
 */
sealed trait CoverParam extends Serializable
{
  val fun : Int => Int = (x) => x
}

trait ALL extends CoverParam
trait ANY extends CoverParam
trait N extends CoverParam
{
  val n: Int
}
