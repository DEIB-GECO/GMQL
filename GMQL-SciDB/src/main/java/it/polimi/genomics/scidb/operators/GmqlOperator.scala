package it.polimi.genomics.scidb.operators

import it.polimi.genomics.scidb.exception.{MissingDataGmqlSciException, InternalErrorGmqlSciException}
import it.polimi.genomics.scidb.utility.StringUtils
import it.polimi.genomics.scidbapi.schema.{Attribute, Dimension}
import it.polimi.genomics.scidbapi.script.{SciCreate, SciOperation, SciScript}
import it.polimi.genomics.scidbapi.{SciStoredArray, SciAbstractArray, SciArray}



/**
  * Provide the definition for a general Gmql it.polimi.genomics.scidb.test.operator. It provides
  * some already implemented methods, too
  */
abstract class GmqlOperator
{
  // ------------------------------------------------------------
  // -- REPRESENTATION ------------------------------------------

  var _array : Option[SciAbstractArray] = None  // contains, if already computed the intermediate result array

  var _stored : Boolean = false                 // is true if the node have to materialize the result into SciDB
  var _storing_temp : Boolean = true            // is true if the array has to be a temp array
  var _storing_name : Option[String] = None     // is the eventual name used to save the intermediate result

  var _usage_counter : Int = 1                  // it counts the number of nodes that depend on the current node
  var _access_counter : Int = 0                 // it counts how many times was called the current node

  // ------------------------------------------------------------
  // -- TYPE ----------------------------------------------------

  /**
    * Returns the theoretic schema of the result, in order to verify it
    * with the actual schema obtained by the computation
    *
    * @return Theoretic schema
    */
  def schema : (List[Dimension], List[Attribute])

  /**
    * This method verify if the array complies the current required
    * schema according to the dimensions and attributes lists
    *
    * @return true if the array complies with the type
    */
  final def check : Boolean =
  { return true
    if( _array.isEmpty ){

      // default ------------------------------------
      false

    }else{

      // check on dimensions ------------------------
      val dimensionsCheck = if( schema._1.isEmpty ) true else
      {
        schema._1.size == _array.get.getDimensions().size &&
          schema._1.map(item => _array.get.getDimensions().contains(item)).reduce(_&&_)
      }

      // check on attributes ------------------------
      val attributesCheck = if( schema._2.isEmpty ) true else
      {
        schema._2.size == _array.get.getAttributes().size &&
          schema._2.map(item => _array.get.getAttributes().contains(item)).reduce(_&&_)
      }

      // result -------------------------------------
      dimensionsCheck && attributesCheck

    }
  }


  // ------------------------------------------------------------
  // -- STATUS --------------------------------------------------

  /**
    * Returns true if the array in the node was already computed
    *
    * @return isComputed status
    */
  final def isComputed : Boolean = _array.isDefined

  /**
    * Return true if the array at node will be materialized into
    * a temporary SciDB array
    *
    * @return isStored status
    */
  final def isStored : Boolean = _stored

  /**
    * Returns, if existi, the intermediate result node
    *
    * @return the current array
    */
  final def array : SciAbstractArray =
  {
    this._array match
    {
      case Some(a) => a
      case (None) => throw new MissingDataGmqlSciException("The node '"+ this +"' was still not computed")
    }
  }


  // ------------------------------------------------------------
  // -- COMPUTATION ---------------------------------------------

  /**
    * Apply the it.polimi.genomics.scidb.test.operator producing the result, it requires the
    * child nodes results if necessary
    *
    * @param script Context script
    */
  def apply(script:SciScript) : SciArray

  /**
    * Computes, if required, the intermediate array for the node.
    * If the array was already computed, it returns the stored
    * array, if not run compute it and subscribe it into the
    * script if necessary
    *
    * @param script Context script
    */
  final def compute(script:SciScript) : SciArray =
  {
    // apply if necessary -------------------------
    if( !isComputed ){

      script.openQueue()

      // compute the array -------------
      var array : SciArray = apply(script)

      // store, if required ------------
      if( _storing_name.isEmpty )
        _storing_name = Some(script.getTmpName +"_"+ this.getClass.getSimpleName)

      if( _stored ) {

        _array = Some(array.store(_storing_name.get))

        if(_storing_temp)
          script.addStatement(SciCreate(
            true,
            _storing_name.get,
            _array.get.getDimensions(),
            _array.get.getAttributes()
          ))

        script.addStatement(_array.get)
        script.flushQueue()

      }else{
        _array = Some(array)
      }

      script.closeQueue()


      // check schema ------------------
      if( !check )
        throw new InternalErrorGmqlSciException("The node '"+ this +"' as computed a intermediate result that doesn't match the expected")

    }

    // record access ------------------------------
    this._access_counter += 1

    if( isStored && _access_counter == _usage_counter )
      script.addQueueStatement(new SciOperation("remove", _array.get.asInstanceOf[SciStoredArray].getAnchor))

    // return result ------------------------------
    this._array match
    {
      case Some(a:SciArray) => a
      case Some(a:SciStoredArray) => a.reference()
      case (None|Some(_)) => throw new InternalErrorGmqlSciException("The node '"+ this +"' has not computed the array")
    }
  }


  // ------------------------------------------------------------
  // -- PREPARATION ---------------------------------------------

  /**
    * Increments the dependencies counter and evaluate the new
    * situation if necessary
    */
  final def use : GmqlOperator =
  {
    this._usage_counter += 1
    this
  }


  // ------------------------------------------------------------
  // -- DESCRIPTION ---------------------------------------------

  /**
    * Provides a complete description of the object
    *
    * @param append string containing to extend the informations
    * @return the description string
    */
  final def description(append:String = "") : String =
  {
    var description = "GmqlOperator["

    description += "\n\ttype: " + this.getClass.getSimpleName
    description += "\n\tarray: " + (if( _array.isEmpty ) "null" else _array.get.toString() )
    description += "\n\tstored: " + _stored
    description += "\n\tdependency counter: " + _usage_counter
    description += "\n\taccess counter: " + _access_counter
    description += append

    description += "\n]"

    description
  }

  /**
    * Returns the object string
    *
    * @return
    */
  final override def toString : String =
  {
    "["+ this.getClass.getSimpleName +" - "+
      "stored:"+ isStored +", "+
        "computed:"+ isComputed +", "+
        "counter:"+ _access_counter +"/" + _usage_counter +
    "]"
  }

}
