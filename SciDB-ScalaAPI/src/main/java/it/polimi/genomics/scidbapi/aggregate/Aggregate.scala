package it.polimi.genomics.scidbapi.aggregate

import java.util.NoSuchElementException

import it.polimi.genomics.scidbapi.exception.{ResourceNotFoundSciException, PropertyAmbiguitySciException, UnsupportedOperationSciException}
import it.polimi.genomics.scidbapi.expression.A
import it.polimi.genomics.scidbapi.schema.DataType._
import it.polimi.genomics.scidbapi.schema.{Attribute, Dimension}

/**
  * Provides the evaluation for aggregation functions
  *
  * @param function aggregation function
  * @param attribute aggregation attribute
  * @param name new attribute name
  */
abstract class Aggregate(function:String, attribute:A, name:A = null)
{
  def eval(context:(List[Dimension],List[Attribute])) : (String, DataType, String) =
  {

    // type check ---------------------------------
    var (attributeString, returnType, returnName) : (String, DataType, String) = (function,attribute) match
    {

      // ------------------------------------------
      // special cases ----------------------------
      case ("count", A("*")) => ("*", UINT64, "count")


      // ------------------------------------------
      // base cases -------------------------------
      case _ => {
        val (attributeString, attributeType) = attribute.eval(context)

        val returnType : DataType = (function, attributeType) match
        {
          // stats
          case ("count", _) => UINT64
          case (("avg"|"stdev"|"var"), (DOUBLE|INT64|BOOL)) => DOUBLE

          // numerical
          case (("prod"|"sum"), (INT64|BOOL)) => INT64
          case (("prod"|"sum"), DOUBLE) => DOUBLE
          case ("sum", STRING) => STRING

          // selection
          case (("max"|"min"|"median"), BOOL) => BOOL
          case (("max"|"min"|"median"), INT64) => INT64
          case (("max"|"min"|"median"), DOUBLE) => DOUBLE
          case (("max"|"min"|"median"), STRING) => STRING

          // default
          case (_, datatype) => throw new UnsupportedOperationSciException("Aggregate not supported, required '" + function + "(" + datatype + ")'")
        }

        (attributeString, returnType, attributeString + "_" + function)
      }


    }

    // result construction ------------------------
    if( name != null && name.isNotInContext(context) )
      returnName = name.value()

    (function +"("+ attributeString +") as "+ returnName , returnType, returnName)
  }
}


// ------------------------------------------------------------


/**
  * General aggregation function, simply call the super method
  *
  * @param function aggregation function
  * @param attribute aggregation attribute
  * @param name new attribute name
  */
case class AGGR(function:String, attribute:A, name:A = null) extends Aggregate(function, attribute, name)
{
  override def eval(context:(List[Dimension],List[Attribute])) : (String, DataType, String) = super.eval(context)
}