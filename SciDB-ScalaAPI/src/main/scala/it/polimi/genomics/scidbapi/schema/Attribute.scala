package it.polimi.genomics.scidbapi.schema

import it.polimi.genomics.scidbapi.expression.A
import it.polimi.genomics.scidbapi.schema.DataType.DataType

/**
  * Represents an array attribute, according to the standard SciDB definition
  *
  * @param name attribute name
  * @param datatype attribute type
  */
case class Attribute(name:String, datatype:DataType, nullable:Boolean = true)
{

  // ------------------------------------------------------------
  // -- OPERATIONS ----------------------------------------------

  /**
    * Returns a disambiguated version of the current attribute
    * checking in the list
    *
    * @param list attribute list
    * @return disambiguated attribute
    */
  def disambiguate(list:List[Attribute]) : Attribute =
  {
    list.exists(_.name == name) match
    {
      case false => this
      case true => rename(name+"_2")
    }
  }

  // ------------------------------------------------------------
  // -- COMPARISON ----------------------------------------------

  /**
    * Verifies if the two attributes coincide
 *
    * @param a second attribute
    * @return true if attributes coincide
    */
  def coincides(a:Attribute) : Boolean = datatype == a.datatype

  /**
    * Verifies if the two attributes are equal
 *
    * @param d second attribute
    * @return true if attributes are equal
    */
  def equal(a:Attribute) : Boolean =
  {
    name == a.name &&
      coincides(a)
  }

  // ------------------------------------------------------------
  // -- CAST ----------------------------------------------------

  /**
    * Tries to build a dimension starting from the attribute
    *
    * @return a Dimension instance
    */
  def toDimension() : Dimension = Dimension.initFromAttribute(this)


  /**
    * Returns a new attribute renaming it
    *
    * @param newName new name
    * @return new attribute
    */
  def rename(newName:String) : Attribute = Attribute(newName, datatype, nullable)

  /**
    * Returns a renamed attribute attaching the label
    *
    * @param label label name
    * @return renamed attribute
    */
  def label(label:String) : Attribute = rename(label+"$"+name)

  /**
    * Returns a renamed attributed to be used with afl disambiguation
    * aliases
    *
    * @param alias alias string
    * @return renamed attribute
    */
  def alias(alias:String) : Attribute = rename(alias+"."+name)

  /**
    * Returns the attribute reference to be used inside an expression
    *
    * @return the expression value
    */
  def e : A = A(this.name)

  // ------------------------------------------------------------
  // -- OVERRIDE ------------------------------------------------

  override def toString() : String =
  {
    name +":"+ datatype +" "+ (if(nullable) "null" else "not null")
  }

  def description() : String =
  {
    "Attribute[name="+ name +", datatype="+ datatype +"]"
  }

}


// -------------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------------


/**
  * Provides factory methods and default values to create attributes
  */
object Attribute
{
  val DEFAULT_NULLABLE : Boolean = true

  /**
    * Initialize a new attribute based on a dimension, in order to preserve information
    * about dimension's properties, bounds and chunks sizes are saves into internal variables
    * to recover them if will be necessary a back conversion
    *
    * @param dimension original dimension
    * @return the new attribute
    */
  def initFromDimension(dimension:Dimension, nullable:Boolean = DEFAULT_NULLABLE) : Attribute =
    Attribute(dimension.name, Dimension.DIMENSION_DATATYPE, nullable)
}
