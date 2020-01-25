package it.polimi.genomics.core.DataStructures.CoverParameters

/**
 * Created by pietro on 08/05/15.
 */
sealed trait CoverParam extends Serializable
{
  val fun : Int => Int = (x) => x
}

trait ALL extends CoverParam {
  override def toString: String = "ALL"
}
trait ANY extends CoverParam {
  override def toString: String = "ANY"
}
trait N extends CoverParam
{
  val n: Int

  override def toString: String = n.toString
}

object CoverParameterManager{
  /**
    * This function is used by the APIs for getting the Cover Parameters. It is necessary because
    * the cover parameters are defined as traits and this means that they must be subclassed every time
    * they are needed. In order not to have a dependency on the API modules we need to put the creation
    * logic in this module...
    * @param paramName name of the cover parameter
    * @param value optional value for the N parameter and identify addend 'm' in expression: (ALL + m) / k
    * @param value2 optional value identify factor 'k' in expression: (ALL + m) / k and ALL / k
    * @return a CoverParam
    */
  def getCoverParam(paramName: String, value: Option[Int] = None, value2:Option[Int] = None): CoverParam = {
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
      case "ALLDIV" => {
        if (value2.isDefined) {
          new ALL {
            override val fun = (a: Int) => a / value2.get
          }
        }
        else
          throw new IllegalArgumentException("If ALLDIV is required, value2 must be filled")
      }
      case "ALLSUMDIV" => {
        if(value.isDefined && value2.isDefined) {
          new ALL {
            override val fun = (a: Int) => (a + value.get) / value2.get
          }
        }
        else
          throw new IllegalArgumentException("If ALLSUMDIV is required, both value must be filled")
      }
      case _ => throw new IllegalArgumentException(paramName + " is an invalid COVER parameter")
    }

  }
}
