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

object CoverParameterManager{
  /**
    * This function is used by the APIs for getting the Cover Parameters. It is necessary because
    * the cover parameters are defined as traits and this means that they must be subclassed every time
    * they are needed. In order not to have a dependency on the API modules we need to put the creation
    * logic in this module...
    * @param paramName name of the cover parameter
    * @param value optional value for the N parameter
    * @return a CoverParam
    */
  def getCoverParam(paramName: String, value: Option[Int]): CoverParam = {
    paramName.toUpperCase match {
      case "ALL" => new ALL{}
      case "ANY" => new ANY{}
      case "N" => {
        if(value.isDefined) {
          new N {
            override val n: Int = value.get
          }
        }
        else
          throw new IllegalArgumentException("If N is required, value must be filled")
      }
      case _ => throw new IllegalArgumentException(paramName + " is an invalid COVER parameter")
    }
  }
}
