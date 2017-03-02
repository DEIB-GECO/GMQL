package it.polimi.genomics.scidbapi

import it.polimi.genomics.scidbapi.schema.{Attribute, Dimension}

/**
  * The class provides the methods avaliavles on stored array.
  * The stored arrays cannot be used inside method provided by the nested
  * array, but can be extracted a reference that can be used as input
  * for other operations
  *
  * @param dimensions the list of the array's dimensions
  * @param attributes the list of the array's attributes
  * @param query the query representing the array, when runned the array
  *              will be correctly materialized as expected
  * @param anchor the stored array name
  */
class SciStoredArray(dimensions:List[Dimension],
                     attributes:List[Attribute],
                     query:String,
                     anchor:String)
  extends SciAbstractArray(dimensions,attributes,query)
{

  /**
    * Returns a new array with the reference to the current
    * stored array. The output query will be the simple anchor
    * @return the reference array
    */
  def reference() : SciArray =
  {
    new SciArray(dimensions,attributes,anchor)
  }

  // ------------------------------------------------------------
  // -- UTILITIES -----------------------------------------------

  def getAnchor() : String = anchor

  // ------------------------------------------------------------
  // -- OVERRIDE ------------------------------------------------

  override def toString() : String = this.description()

  def description() : String = super.description("\n\tanchor: " + anchor)

}
