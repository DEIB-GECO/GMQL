package it.polimi.genomics.scidbapi

import it.polimi.genomics.scidbapi.schema.{Attribute, Dimension}
import it.polimi.genomics.scidbapi.script.SciStatement

/**
  * This is an abstract class to define the SciDB's array representation
  * inside Scala
  *
  * @param dimensions the list of the array's dimensions
  * @param attributes the list of the array's attributes
  * @param query the query representing the array, when runned the array
  *              will be correctly materialized as expected
  */
abstract class SciAbstractArray(dimensions:List[Dimension],
                                attributes:List[Attribute],
                                query:String)
  extends SciStatement
{
  def getDimensions() : List[Dimension] = dimensions
  def getAttributes() : List[Attribute] = attributes
  def getQuery() : String = query

  override def getStatementQuery() : String = query

  // ------------------------------------------------------------
  // -- OVERRIDE ------------------------------------------------

  override def toString() : String = super.toString()

  def description(append:String = "") : String =
  {
    var description = "SciArray["

    description += "\n\tdimensions:\t" + dimensions.head
    for(dimension <- dimensions.tail){
      description += "\n\t\t\t\t" + dimension
    }

    description += "\n\tattributes:\t" + attributes.head
    for(attribute <- attributes.tail){
      description += "\n\t\t\t\t" + attribute
    }

    description += "\n\tquery:\n" + query.tab().tab()

    description += append

    description += "\n]"

    description
  }

  def shape() : String = dimensions +" - "+ attributes

}
