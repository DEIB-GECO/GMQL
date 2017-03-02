package it.polimi.genomics.scidbapi.schema

import it.polimi.genomics.scidbapi.exception.IllegalOperationSciException
import it.polimi.genomics.scidbapi.expression.{D, A}

/**
  * Represents an array dimension, according to the standard SciDB definition
  *
  * @param name dimension name
  * @param dim_lo lower bound for dimension
  * @param dim_hi upper bound for dimension (no limit if missing)
  * @param chunk_length chunk size for the dimension
  * @param overlap chunk overlap for the dimension
  */
case class Dimension(name:String, dim_lo:Int, dim_hi:Option[Int], chunk_length:Int, overlap:Int)
{

  // ------------------------------------------------------------
  // -- OPERATIONS ----------------------------------------------

  /**
    * Generates a new dimension starting from two matching dimension
    * and making the intersection of the sizes
    *
    * @param right right dimension in the operation
    * @return the new dimension
    */
  def intersection(right:Dimension) : Dimension =
  {
    if( !( this.matches(right) ) )
      throw new IllegalOperationSciException("Invalid intersecation on dimensions, dimensions '"+ this +"' and '"+ right +"' don't match")

    val dim_lo_res = Math.max(dim_lo, right.dim_lo)
    val dim_hi_res = (dim_hi, right.dim_hi) match {
      case (None, None) => None
      case (Some(l), None) => Some(l)
      case (None, Some(r)) => Some(r)
      case (Some(l),Some(r)) => Some(Math.min(l,r))
    }

    if(dim_hi_res.isDefined && dim_hi_res.get < dim_lo_res)
      throw new IllegalOperationSciException("Invalid intersecation on dimensions, the resulting dimension starting from '"+ this +"' and '"+ right +"' has a lower size greather than the higher size")

    new Dimension(name, dim_lo_res, dim_hi_res, chunk_length, overlap)
  }

  /**
    * Generates a new dimension starting from two matching dimension
    * and making the union of the sizes
    *
    * @param right right dimension in the operation
    * @return the new dimension
    */
  def union(right:Dimension) : Dimension =
  {
    if( !( this.matches(right) ) )
      throw new IllegalOperationSciException("Invalid union on dimensions, dimensions '"+ this +"' and '"+ right +"' don't match")

    val dim_lo_res = Math.min(dim_lo, right.dim_lo)
    val dim_hi_res = (dim_hi, right.dim_hi) match {
      case (Some(l),Some(r)) => Some(Math.max(l,r))
      case _ => None
    }

    if(dim_hi_res.isDefined && dim_hi_res.get < dim_lo_res)
      throw new IllegalOperationSciException("Invalid union on dimensions, the resulting dimension starting from '"+ this +"' and '"+ right +"' has a lower size greather than the higher size")

    new Dimension(name, dim_lo_res, dim_hi_res, chunk_length, overlap)
  }

  /**
    * Returns a disambiguated version of the current dimension
    * checking in the list
    *
    * @param list dimension list
    * @return disambiguated dimension
    */
  def disambiguate(list:List[Dimension]) : Dimension =
  {
    list.exists(_.name == name) match
    {
      case false => this
      case true => new Dimension(name+"_2", dim_lo, dim_hi, chunk_length, overlap)
    }
  }

  // ------------------------------------------------------------
  // -- COMPARISON ----------------------------------------------

  /**
    * Verifies if the two dimensions match
    *
    * @param d second dimension
    * @return true if dimensions match
    */
  def matches(d:Dimension) : Boolean =
  {
    chunk_length == d.chunk_length &&
      overlap == d.overlap
  }

  /**
    * Verifies if the two dimensions coincide
    *
    * @param d second dimension
    * @param semi true if it's required a semi coincidence
    * @return true if dimensions coincide
    */
  def coincides(d:Dimension, semi:Boolean = false) : Boolean =
  {
   dim_lo == d.dim_lo &&
      (dim_hi == dim_hi || semi) &&
      chunk_length == d.chunk_length
  }

  /**
    * Verifies if the two dimensions are equal
    *
    * @param d second dimension
    * @return true if dimensions are equal
    */
  def equal(d:Dimension) : Boolean =
  {
    name == d.name &&
      coincides(d)
  }

  // ------------------------------------------------------------
  // -- CAST ----------------------------------------------------

  /**
    * Builds a new attribute starting from a dimension
    *
    * @return an Attribute instance
    */
  def toAttribute(nullable:Boolean = Attribute.DEFAULT_NULLABLE) : Attribute =
    Attribute.initFromDimension(this, nullable)

  /**
    * Return a dimension as a copy of the current one but
    * with a different name
    *
    * @param name new name
    * @return renamed dimension
    */
  def rename(name:String) : Dimension = new Dimension(name, this.dim_lo, this.dim_hi, this.chunk_length, this.overlap)

  /**
    * Returns a renamed dimension attaching the label
    *
    * @param label label name
    * @return renamed dimension
    */
  def label(label:String) : Dimension = rename(label+"$"+name)

  /**
    * Returns a renamed dimension to be used with afl disambiguation
    * aliases
    *
    * @param alias alias string
    * @return renamed dimension
    */
  def alias(alias:String) : Dimension = rename(alias+"."+name)

  /**
    * Returns the dimension reference to be used inside an expression
    *
    * @return the expression value
    */
  def e : D = D(this.name)

  // ------------------------------------------------------------
  // -- OVERRIDE ------------------------------------------------

  override def toString() : String =
  {
    name + " = " +
      dim_lo + ":" + ( if(dim_hi.isDefined) dim_hi.get else "*") + "," +
      chunk_length + "," + overlap
  }

  def description() : String =
  {
    "Dimension[name=" + name +
      ", dim_lo=" + dim_lo + ", dim_lo=" + ( if(dim_hi.isDefined) dim_hi.get else "*") +
      ", chunk_length=" + chunk_length + ", overlap=" + overlap +"]"
  }

}


// -------------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------------


/**
  * Provides factory methods and default values to create dimensions
  */
object Dimension
{
  val DIMENSION_DATATYPE = DataType.INT64

  val DEFAULT_DIM_LO = 0
  val DEFAULT_DIM_HI = None
  val DEFAULT_CHUNK_LENGTH = 1000000
  val DEFAULT_OVERLAP = 0

  /**
    * Initialize a new dimension based on an attribute
 *
    * @param attribute original attribute
    * @return the new dimension
    */
  def initFromAttribute(attribute:Attribute,
                        dim_lo:Int = DEFAULT_DIM_LO,
                        dim_hi:Option[Int] = DEFAULT_DIM_HI,
                        chunk_length:Int = DEFAULT_CHUNK_LENGTH,
                        overlap:Int = DEFAULT_OVERLAP) : Dimension = attribute match
  {
    case Attribute(name, DataType.INT64, true) => Dimension(name, dim_lo, dim_hi, chunk_length, overlap)
    case Attribute(name, DataType.INT64, false) => throw new IllegalOperationSciException("Attribute '"+ name +"' could be not used as dimension, attribute should be nullable to be used as dimension")
    case Attribute(name, datatype, _) => throw new IllegalOperationSciException("Attribute '"+ name +"' could be not used as dimension, attribute type is '"+ datatype +"' instead of 'int64'")
  }

}
