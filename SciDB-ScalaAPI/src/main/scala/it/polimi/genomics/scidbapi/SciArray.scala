package it.polimi.genomics.scidbapi

import java.nio.file.Paths

import it.polimi.genomics.scidbapi.SortingKeyword.SortingKeyword
import it.polimi.genomics.scidbapi.aggregate.Aggregate
import it.polimi.genomics.scidbapi.exception._
import it.polimi.genomics.scidbapi.expression.{A, D, Expr}
import it.polimi.genomics.scidbapi.schema.DataType._
import it.polimi.genomics.scidbapi.schema.{Attribute, DataType, Dimension}

/**
  * The class provides the methods availables on a nested array.
  * Each array represented as SciArray can be passed as argument
  * for other operations
  * All the methods returns a new array based on the original, and
  * doesn't modifies the original one.
  *
  * @param dimensions the list of the array's dimensions
  * @param attributes the list of the array's attributes
  * @param query the query representing the array, when runned the array
  *              will be correctly materialized as expected
  */
class SciArray(dimensions:List[Dimension],
               attributes:List[Attribute],
               query:String)
  extends SciAbstractArray(dimensions,attributes,query)
{

  // ------------------------------------------------------------
  // -- AFL OPERATORS -------------------------------------------

  /**
    * Returns a new array applying the aggregation rules required
    *
    * @param aggregationCalls aggregation calls
    * @param aggregationDimensions aggregation dimensions, each aggregation
    *                              function will be applied maintaining the
    *                              specified dimensions
    * @return array containing the aggregation values
    * @see <a href="http://paradigm4.com/HTMLmanual/15.7/scidb_ug/re01.html">Aggregate specification</a>
    */
  def aggregate(aggregationCalls:Aggregate*)
               (aggregationDimensions:D*) : SciArray =
  {
    val context = (dimensions, attributes)

    // dimension evaluation -----------------------
    val extractedDimensions = aggregationDimensions.map((item:D) => item.extract(context))

    // calls evaluation ---------------------------
    if(aggregationCalls.isEmpty)
      throw new IllegalOperationSciException("The aggregate statement should have at least one aggregate call")

    val evaluatedCalls = aggregationCalls.map((item:Aggregate) => item.eval(context))

    val duplicatedCalls = evaluatedCalls.map(_._3).diff( evaluatedCalls.map(_._3).distinct )
    if( duplicatedCalls.nonEmpty )
      throw new PropertyAmbiguitySciException("The attributes "+ duplicatedCalls.map("'" + _ + "'").mkString(",") +" already exists, please define distinct name for new attributes")

    // new array construction ---------------------
    var resultDimensions = extractedDimensions.toList
    var resultAttributes = evaluatedCalls.map((item:(String,DataType,String)) => new Attribute(item._3, item._2)).toList

    if(resultDimensions.isEmpty)
      resultDimensions = List[Dimension]( new Dimension("i",0,Some(0),1,0) )

    // query construction -------------------------
    var resultQuery = "aggregate(\n" + query.tab() + ","
    resultQuery += "\n\t" + evaluatedCalls.map(_._1).mkString(",\n\t")

    if(extractedDimensions.nonEmpty)
      resultQuery += ",\n\t" + aggregationDimensions.map(_.value()).mkString(",")

    resultQuery += "\n)"

    // output -------------------------------------
    new SciArray(resultDimensions, resultAttributes, resultQuery)
  }


  /**
    * Returns a new array applying the required expression to add to the
    * schema new attributes
    *
    * @param appliedAttributes couples of value of type attribute name - expression
    *                          that defines the new names and the related expressions
    * @return the produced array
    * @see <a href="http://paradigm4.com/HTMLmanual/15.7/scidb_ug/re02.html">Apply specification</a>
    */
  def apply(appliedAttributes:(A,Expr)*) : SciArray =
  {
    val context = (dimensions, attributes)

    // attributes evaluation ----------------------
    if(appliedAttributes.isEmpty)
      throw new IllegalOperationSciException("The apply statement should have at least one new attribute")

    val duplicatedAttributes = appliedAttributes.map(_._1.value()).diff( appliedAttributes.map(_._1.value()).distinct )
    if( duplicatedAttributes.nonEmpty )
      throw new PropertyAmbiguitySciException("The attributes "+ duplicatedAttributes.map("'" + _ + "'").mkString(",") +" already exists, please define distinct name for new attributes")

    appliedAttributes.map(item => item._1.isNotInContext(context))

    val evaluatedAttributes = appliedAttributes.map( item => (item._1.value(), item._2.eval(context)) )

    // new array construction ---------------------
    var resultAttributes = attributes ::: evaluatedAttributes.map( item => new Attribute(item._1, item._2._2) ).toList

    // query construction -------------------------
    var resultQuery = "apply(\n" + query.tab() + ","
    resultQuery += "\n\t" + evaluatedAttributes.map(item => item._1 + ", " + item._2._1).mkString(",\n\t")
    resultQuery += "\n)"

    // output -------------------------------------
    new SciArray(dimensions, resultAttributes, resultQuery)
  }


  /**
    * Returns a new array applying the dimensions and attributs casting
    *
    * @param castDimensions new dimensions schema
    * @param castAttributes new attributes schema
    * @return the casted array
    * @see <a href="http://paradigm4.com/HTMLmanual/15.7/scidb_ug/castOperator.html">Cast specification</a>
    */
  def cast(castDimensions:List[Dimension], castAttributes:List[Attribute]) : SciArray =
  {
    // dimensions evaluation ----------------------
    if( dimensions.size != castDimensions.size )
      throw new IllegalOperationSciException("To apply array casting it's required the same number of dimensions")

    val resultDimensions = dimensions.zip(castDimensions).map(item => {
      if(item._1.coincides(item._2))
        item._2
      else
        throw new IllegalOperationSciException("Array casting is possible only using the same dimension types, dimension '"+ item._1 +"' is not compatible with '"+ item._2 +"'")
    })

    // attributes evaluation ----------------------
    if( attributes.size != castAttributes.size )
      throw new IllegalOperationSciException("To apply array casting it's required the same number of attributes")

    val resultAttributes = attributes.zip(castAttributes).map(item => {
      if(item._1.coincides(item._2))
        item._2
      else
        throw new IllegalOperationSciException("Array casting is possible only using the same attribute types, attribute '"+ item._1 +"' is not compatible with '"+ item._2 +"'")
    })

    // query construction -------------------------
    var resultQuery = "cast(\n" + query.tab() + ","
    resultQuery += "\n\t<" + resultAttributes.map(_.toString()).mkString(", ") + ">"
    resultQuery += "\n\t\t[" + resultDimensions.map(_.toString()).mkString(", ") + "]"
    resultQuery += "\n)"

    // output -------------------------------------
    new SciArray(resultDimensions, resultAttributes, resultQuery)
  }


  /**
    * Retyrbs a new array resulting from the join of the current array and an
    * external one, on required dimensions
    *
    * @param right right external array
    * @param aliasLeft alias for the current left array
    * @param aliasRight alias for the external right array
    * @param joinDimensions list of dimensions to perform the join
    * @return the joined array
    * @see <a href="http://paradigm4.com/HTMLmanual/15.7/scidb_ug/crossJoinOperator.html">Cross join specification</a>
    */
  def cross_join(right:SciArray, aliasLeft:String = "", aliasRight:String = "")
                (joinDimensions:(D,D)*) : SciArray =
  {
    val leftContext = (dimensions, attributes)
    val rightContext = (right.getDimensions(), right.getAttributes())

    // dimensions evaluation ----------------------
    val evalJoinDimensions = joinDimensions.map(item => {
      val leftDim = D(item._1.value().unalias(aliasLeft)).extract(leftContext)
      val rightDim = D(item._2.value().unalias(aliasRight)).extract(rightContext)

      if( !( leftDim.matches(rightDim) ) )
        throw new IllegalOperationSciException(
          "Invalid cross_join, dimensions '"+
            (if(aliasLeft != "") aliasLeft+"." else "") + leftDim +"' and '"+
            (if(aliasRight != "") aliasRight+"." else "") + rightDim +"' don't match")

      (leftDim, rightDim, leftDim.intersection(rightDim))
    })

    // new dimensions construction ----------------
    var resultDimensions = dimensions.map(item =>
      evalJoinDimensions.find(_._1.equal(item)) match {
        case Some(dim) => dim._3
        case None => item
      })

    resultDimensions =
      resultDimensions :::
      right.getDimensions()
        .diff(evalJoinDimensions.map(_._2))
        .map(_.disambiguate(resultDimensions))

    // new attributes construction ----------------
    var resultAttributes =
      attributes :::
      right.getAttributes().map(_.disambiguate(attributes))

    // query construction -------------------------
    var resultQuery = "cross_join(\n\n" + query.tab() + (if(aliasLeft != "") " as " + aliasLeft else "") + ","
    resultQuery += "\n\n" + right.getQuery().tab() + (if(aliasRight != "") " as " + aliasRight else "")

    if(joinDimensions.isEmpty)
      resultQuery += "\n"
    else
      resultQuery += ",\n\n\t" + joinDimensions.map(item => item._1.value() + ", " + item._2.value()).mkString(",\n\t")

    resultQuery += "\n)"

    // output -------------------------------------
    new SciArray(resultDimensions, resultAttributes, resultQuery)
  }


  /**
    *
    *
    * @param aggregationCalls
    * @param dimension
    * @return
    */
  def cumulate(aggregationCalls:Aggregate*)(dimension:D = null) : SciArray =
  {
    val context = (dimensions, attributes)

    // dimension evaluation -----------------------
    val extractedDimensions = if(dimension != null) dimension.extract(context) else null

    // calls evaluation ---------------------------
    if(aggregationCalls.isEmpty)
      throw new IllegalOperationSciException("The aggregate statement should have at least one aggregate call")

    val evaluatedCalls = aggregationCalls.map((item:Aggregate) => item.eval(context))

    val duplicatedCalls = evaluatedCalls.map(_._3).diff( evaluatedCalls.map(_._3).distinct )
    if( duplicatedCalls.nonEmpty )
      throw new PropertyAmbiguitySciException("The attributes "+ duplicatedCalls.map("'" + _ + "'").mkString(",") +" already exists, please define distinct name for new attributes")

    // new array construction ---------------------
    var resultDimensions = dimensions
    var resultAttributes = evaluatedCalls.map((item:(String,DataType,String)) => new Attribute(item._3, item._2)).toList

    // query construction -------------------------
    var resultQuery = "cumulate(\n" + query.tab() + ","
    resultQuery += "\n\t" + evaluatedCalls.map(_._1).mkString(",\n\t")

    if(extractedDimensions != null)
      resultQuery += ",\n\t" + dimension.value()

    resultQuery += "\n)"

    // output -------------------------------------
    new SciArray(dimensions, resultAttributes, resultQuery)
  }


  /**
    * Returns a new array applying the desired filter
    *
    * @param expression the filter expression
    * @return the filtered array
    * @see <a href="http://paradigm4.com/HTMLmanual/15.7/scidb_ug/filterOperator.html">Filter specification</a>
    */
  def filter(expression:Expr) : SciArray =
  {
    val context = (dimensions, attributes)

    // expression evaluation ----------------------
    val expressionString = expression.eval(context) match {
      case (string, DataType.BOOL) => string
      case (string, datatype) => throw new IllegalOperationSciException("The filter statement should have boolean expression, required '"+string+":"+datatype+"'")
    }

    // query construction -------------------------
    var resultQuery = "filter(\n" + query.tab() + ","
    resultQuery += "\n\t" + expressionString
    resultQuery += "\n)"

    // output -------------------------------------
    new SciArray(dimensions, attributes, resultQuery)
  }


  /**
    * Retruns a new array applying the index id to the current array
    *
    * @param index index array
    * @param attr indexed attribute in the current array
    * @param output name for the new attribute containing the index id
    * @param sorted true if the index array is sorted and dense
    * @return the current array with the index attribute
    * @see <a href="http://www.paradigm4.com/HTMLmanual/15.7/scidb_ug/index_lookup.html">Index lookup specification</a>
    */
  def index_lookup(index:SciArray, attr:A, output:A = null, sorted:Boolean = false ) : SciArray =
  {
    val context = (dimensions, attributes)
    val alias = "__INPUT"

    // index evaluation ---------------------------
    if( index.getDimensions().size != 1 )
      throw new IllegalOperationSciException("The index_lookup statement require a one-dimensional index, the current array has "+ index.getDimensions().size +" dimensions")

    if( index.getAttributes().size > 1 )
      throw new IllegalOperationSciException("The index_lookup statement require a one-attribute index, the current array has "+ index.getAttributes().size + " attributes")

    if( index.getAttributes().head.nullable )
      throw new IllegalOperationSciException("The index_lookup statement require a non-nullable one-attribute index")

    // input evaluation ---------------------------
    if( !attr.extract(context).coincides( index.getAttributes().head ) )
      throw new ResourceConflictSciException("Attribute '"+ attr.value() +"' not coincides with the index attribute: '"+ index.getAttributes().head +"'")

    val indexAttr = try{
      output.eval(context)
      throw new ResourceConflictSciException("Attribute '"+ output.value() +"' already is already present in the array, please use a different name")
    }catch{
      case ex:ResourceNotFoundSciException => new Attribute(output.value(), INT64)
      case ex:NullPointerException => new Attribute(attr.value()+"_index", INT64)
    }
    val resultAttributes = attributes ::: List(indexAttr)

    // query construction -------------------------
    var resultQuery = "index_lookup("
    resultQuery += "\n"+ query.tab() +" as "+ alias +","
    resultQuery += "\n"+ index.getQuery().tab() +","
    resultQuery += "\n\t"+ alias +"."+ attr.value() +","
    resultQuery += "\n\t"+ indexAttr.name +","
    resultQuery += "\n\t'index_sorted="+ (if(sorted) "true" else "false") +"'"
    resultQuery += "\n)"

    // output -------------------------------------
    new SciArray(dimensions, resultAttributes, resultQuery)
  }


  /**
    * Returns a new array obtained merging the current left array
    * with the required external right array
    *
    * @param right externar right array
    * @return the new array
    * @see <a href="http://www.paradigm4.com/HTMLmanual/15.7/scidb_ug/mergeOperator.html">Filter specification</a>
    */
  def merge(right:SciArray) : SciArray =
  {
    // dimensions evaluation ----------------------
    if( dimensions.size != right.getDimensions().size )
      throw new IllegalOperationSciException("The merge statement requires the same number of dimensions")

    val resultDimensions = dimensions.zip(right.getDimensions()).map(item => {
      if(item._1.coincides(item._2, true))
        item._1.union(item._2)
      else
        throw new IllegalOperationSciException("The merge statement requires the same dimension types, dimension '"+ item._1 +"' is not semi-compatible with '"+ item._2 +"'")
    })

    // attributes evaluation ----------------------
    if( attributes.size != right.getAttributes().size )
      throw new IllegalOperationSciException("The merge statement requires the same number of attributes")

    val resultAttributes = attributes.zip(right.getAttributes()).map(item => {
      if(item._1.coincides(item._2))
        item._1
      else
        throw new IllegalOperationSciException("The merge statement requires the same attribute types, attribute '"+ item._1 +"' is not compatible with '"+ item._2 +"'")
    })

    // query construction -------------------------
    var resultQuery = "merge("
    resultQuery += "\n" + query.tab() + ","
    resultQuery += "\n" + right.getQuery().tab()
    resultQuery += "\n)"

    // output -------------------------------------
    new SciArray(resultDimensions, resultAttributes, resultQuery)
  }


  /**
    * Returns a new array with the same dimensions and the required subset
    * of attributes
    *
    * @param projectedAttributes subset of required attributes
    * @return the resulting array
    * @see <a href="http://paradigm4.com/HTMLmanual/15.7/scidb_ug/projectOperator.html">Project specification</a>
    */
  def project(projectedAttributes:A*) : SciArray =
  {
    val context = (dimensions, attributes)

    // attribute evaluation -----------------------
    if(projectedAttributes.isEmpty)
      throw new IllegalOperationSciException("The project statement should have at least one attribute")

    val resultAttributes = projectedAttributes.map((item:A) => item.extract(context)).toList

    // query construction -------------------------
    var resultQuery = "project(\n" + query.tab() + ","
    resultQuery += "\n\t" + projectedAttributes.map(_.value()).mkString(", ")
    resultQuery += "\n)"

    // output -------------------------------------
    new SciArray(dimensions, resultAttributes, resultQuery)
  }


  /**
    * Returns a new array with the ranking on a single attribute of the
    * source array
    *
    * @param rankingAttributeName ranking attribute, if not specified the
    *                             first attribute in the schema will be considered
    * @param groupingDimensions optional list of grouping dimendion for the ranking
    *                           in this case, each combination of single values for
    *                           specified dimensions have a own ranking value
    * @return the new array containing the required ranking
    * @see <a href="http://www.paradigm4.com/HTMLmanual/15.7/scidb_ug/re37.html">Rank specification</a>
    */
  def rank(rankingAttributeName:A = null)
          (groupingDimensions:D*) : SciArray =
  {
    val context = (dimensions, attributes)

    // attribute evaluation -----------------------
    val rankingAttribute = rankingAttributeName match {
      case null => attributes.head
      case name => name.extract(context)
    }

    // dimensions evaluation ----------------------
    groupingDimensions.map(_.eval(context))

    // new array costruction ----------------------
    var resultAttributes = List[Attribute]( rankingAttribute, new Attribute(rankingAttribute.name+"_rank", DOUBLE) )

    // query construction -------------------------
    var resultQuery = "rank(\n" + query.tab() + ","
    resultQuery += "\n\t" + rankingAttribute.name

    if(groupingDimensions.nonEmpty)
      resultQuery += ",\n\t" + groupingDimensions.map(_.value()).mkString(", ")

    resultQuery += "\n)"

    // output -------------------------------------
    new SciArray(dimensions, resultAttributes, resultQuery)
  }


  /**
    * Returns a new array redimensioned like the specified schema
    *
    * @param reDimensions new dimensions schema
    * @param reAttributes new attributes schema
    * @param isStrict true if the script should fail on collisions
    * @param aggregates aggregation calls on collided cells
    * @return the redimensioned array
    * @see <a href="http://paradigm4.com/HTMLmanual/15.7/scidb_ug/redimensionOperator.html">Redimension specification</a>
    */
  def redimension(reDimensions:List[Dimension], reAttributes:List[Attribute], isStrict:Boolean, aggregates:Aggregate*) : SciArray =
  {
    val context = (dimensions, attributes)

    // aggregates evaluation ----------------------
    val evaluatedAggregates = aggregates.map((item:Aggregate) => item.eval(context))

    // dimensions evaluation ----------------------
    val resultDimensions = reDimensions.map(item => {
      if(
        dimensions.exists(d => item.equal(d)) ||
        attributes.exists(a => (item.name == a.name && a.datatype == INT64))
      ) item
      else throw new ResourceNotFoundSciException("Redimension should be based on existing properties(attributes or dimensions), property '"+ item.name +":int64' not found")
    })

    // attributes evaluation ----------------------
    val resultAttributes = reAttributes.map(item => {
      if(
        dimensions.exists(d => (item.name == d.name && item.datatype == INT64)) ||
        attributes.exists(a => item.equal(a)) ||
        evaluatedAggregates.exists(a => (item.name == a._3 && item.datatype == a._2))
      ) item
      else throw new ResourceNotFoundSciException("Redimension should be based on existing properties(attributes or dimensions), property '"+ item.name +":"+item.datatype+"' not found")
    })

    // query construction -------------------------
    var resultQuery = "redimension(\n" + query.tab() + ","
    resultQuery += "\n\t<" + resultAttributes.map(_.toString()).mkString(", ") + ">"
    resultQuery += "\n\t\t[" + resultDimensions.map(_.toString()).mkString(", ") + "]"

    resultQuery += ",\n\t" + (if(isStrict) "true" else "false") + ""

    if( evaluatedAggregates.nonEmpty )
      resultQuery += ",\n\t" + evaluatedAggregates.map(_._1).mkString(",\n\t")

    resultQuery += "\n)"

    // output -------------------------------------
    new SciArray(resultDimensions, resultAttributes, resultQuery)
  }

  def redimension(reDimensions:List[Dimension], reAttributes:List[Attribute]) : SciArray =
    redimension(reDimensions, reAttributes, false)


  /**
    * Returns a new stored array, the current array is saved into
    * file as required, the anchor refears to the query that has
    * generated the saved file
    *
    * @param path complete path of the destination file
    * @param instance instance for performing the saving
    * @param format output data file format
    * @return the saved array
    * @see <a href="http://www.paradigm4.com/HTMLmanual/15.7/scidb_ug/saveOperator.html">Save specification</a>
    */
  def save(path:String, instance:Int = -2, format:String = "text") : SciStoredArray =
  {
    // arguments validation -----------------------
    format match
    {
      case ("csv"|"opaque"|"text"|"tsv") => true
      case _ => throw new UnsupportedOperationSciException("Unsupported input format, '"+ format +"' is not valid")
    }

    // query construction -------------------------
    var resultQuery = "save(\n" + query.tab() + ","
    resultQuery += "\n\t'"+ path +"',"
    resultQuery += "\n\t"+ instance + ", '"+ format +"'"
    resultQuery += "\n)"

    // output -------------------------------------
    new SciStoredArray(dimensions, attributes, resultQuery, query)
  }


  /**
    * Returns a new array where the cells inside the original
    * array are sorted according to the sorting list
    *
    * @param sortingAttributes sorting list specifing the order
    *                          used to evaluate the attributes values
    * @param chunk_size desired chunk size on the single output dimension
    * @return the sorted array
    * @see <a href="http://www.paradigm4.com/HTMLmanual/15.7/scidb_ug/sortOperator.html">Sort specification</a>
    */
  def sort(sortingAttributes:(A,SortingKeyword)*)
          (chunk_size:Int = 0) : SciArray =
  {
    val context = (dimensions, attributes)

    // attribute evaluation -----------------------
    sortingAttributes.map(_._1.eval(context))

    // new array costruction ----------------------
    val resultDimensions = List[Dimension]( new Dimension("n", 0, None, (if(chunk_size>0) chunk_size else Dimension.DEFAULT_CHUNK_LENGTH), 0) )

    // query construction -------------------------
    var resultQuery = "sort(\n" + query.tab()

    if( sortingAttributes.nonEmpty )
      resultQuery += ",\n\t" + sortingAttributes.map(item => item._1.value() +" "+ item._2).mkString(", ")

    if( chunk_size > 0 )
      resultQuery += ",\n\t" + chunk_size

    resultQuery += "\n)"

    // output -------------------------------------
    new SciArray(resultDimensions, attributes, resultQuery)
  }


  /**
    * Returns a new stored array using the required name
    *
    * @param reference new array name
    * @return the stored array
    * @see <a href="http://paradigm4.com/HTMLmanual/15.7/scidb_ug/store.html">Store specification</a>
    */
  def store(anchor:String) : SciStoredArray =
  {
    // anchor name evaluation ---------------------
    if( !anchor.matches("^([A-Za-z0-9_]{0,127})$") )
      throw new SyntaxErrorSciException("'"+ anchor +"' is not a valid name for SciDB arrays, allowed only 'A-Z','a-z','0-9','_'")

    // query construction -------------------------
    var resultQuery = "store(\n" + query.tab() + ","
    resultQuery += "\n\t" + anchor
    resultQuery += "\n)"

    // output -------------------------------------
    new SciStoredArray(dimensions, attributes, resultQuery, anchor)
  }


  def insert(target:SciStoredArray) : SciStoredArray =
  {
    // query construction -------------------------
    var resultQuery = "insert(\n" + query.tab() + ","
    resultQuery += "\n\t" + target.getAnchor()
    resultQuery += "\n)"

    new SciStoredArray(dimensions, attributes, resultQuery, target.getAnchor())
  }


  def between(limits:(Int,Int)*) : SciArray =
  {
    if(limits.size != dimensions.size)
      throw new IllegalOperationSciException("Limits.size error")

    // query construction -------------------------
    var resultQuery = "between(\n" + query.tab() + ", "
    resultQuery += "\n\t" + limits.map(b => if (b._1 >= 0) b._1 else "null").mkString(", ") + ", "
    resultQuery += "\n\t" + limits.map(b => if (b._2 >= 0) b._2 else "null").mkString(", ")
    resultQuery += "\n)"

    // output -------------------------------------
    new SciArray(dimensions, attributes, resultQuery)
  }


  /**
    * Returns a new array with all duplicate values removed, the source
    * array has to be one-dimensional and one-attribute
    *
    * @param chunk_size desired chuck size on the single output dimension
    * @return the new array with all duplicate values removed
    * @see <a href="http://www.paradigm4.com/HTMLmanual/15.7/scidb_ug/uniq.html">Uniq specification</a>
    */
  def uniq(chunk_size:Int = 0) : SciArray =
  {
    // properties evaluation ----------------------
    if( dimensions.size > 1 )
      throw new IllegalOperationSciException("The uniq statement require a one-dimensional array, the current array has "+ dimensions.size +" dimensions")

    if( attributes.size > 1 )
      throw new IllegalOperationSciException("The uniq statement require a one-attribute array, the current array has "+ attributes.size + " attributes")

    // new array costruction ----------------------
    val resultDimensions = List[Dimension]( new Dimension("i", 0, None, (if(chunk_size>0) chunk_size else Dimension.DEFAULT_CHUNK_LENGTH), 0) )

    // query construction -------------------------
    var resultQuery = "uniq(\n" + query.tab()

    if( chunk_size > 0 )
      resultQuery += ",\n\t'chunk_size="+ chunk_size +"'"

    resultQuery += "\n)"

    // output -------------------------------------
    new SciArray(resultDimensions, attributes, resultQuery)
  }


  /**
    * Returns a new array redimensioned with all dimension values as attribute
    * and a synthetic dimension to list data
    *
    * @param dimension dimensions to maintain in the result
    * @param chunk_size desired chuck size on the single output dimension
    * @return the new unpacked array
    * @see <a href="http://www.paradigm4.com/HTMLmanual/15.7/scidb_ug/unpackOperator.html">Unpack specification</a>
    */
  def unpack(dimension:D, chunk_size:Int = 0) : SciArray =
  {
    val context = (dimensions, attributes)

    // properties evaluation ----------------------
    try{
      dimension.eval(context)
      throw new PropertyAmbiguitySciException("The dimension '"+ dimension.value() +"' already exists, please define a different name")
    }catch{
      case ex:ResourceNotFoundSciException => true
    }

    // dimensions construction --------------------
    val resultDimensions = List[Dimension](
      Dimension(dimension.value(), 0, None, if(chunk_size > 0) chunk_size else Dimension.DEFAULT_CHUNK_LENGTH, Dimension.DEFAULT_OVERLAP)
    )

    // attributes construction --------------------
    val resultAttributes = dimensions.map(_.toAttribute()) ::: attributes

    // query construction -------------------------
    var resultQuery = "unpack(\n" + query.tab()
    resultQuery += ",\n\t" + dimension.value()
    if( chunk_size > 0 )
      resultQuery += ","+ chunk_size
    resultQuery += "\n)"

    // output -------------------------------------
    new SciArray(resultDimensions, resultAttributes, resultQuery)
  }


  def variable_window(dimension:D, left:Int, right:Int, aggregationCalls: Aggregate*) : SciArray =
  {
    val context = (dimensions, attributes)

    // dimension evaluation -----------------------
    dimension.eval(context)

    // aggregates evaluation ----------------------
    if(aggregationCalls.isEmpty)
      throw new IllegalOperationSciException("The window statement should have at least one aggregate call")

    val evaluatedCalls = aggregationCalls.map((item:Aggregate) => item.eval(context))

    val duplicatedCalls = evaluatedCalls.map(_._3).diff( evaluatedCalls.map(_._3).distinct )
    if( duplicatedCalls.nonEmpty )
      throw new PropertyAmbiguitySciException("The attributes "+ duplicatedCalls.map("'" + _ + "'").mkString(",") +" already exists, please define distinct name for new attributes")

    // new array construction ---------------------
    var resultAttributes = evaluatedCalls.map((item:(String,DataType,String)) => new Attribute(item._3, item._2)).toList

    // query construction -------------------------
    var resultQuery = "variable_window(\n"+ query.tab() +","
    resultQuery += "\n\t"+ dimension.value +", "+ left +", "+ right + ","
    resultQuery += "\n\t" + evaluatedCalls.map(_._1).mkString(",\n\t")
    resultQuery += "\n)"

    // output -------------------------------------
    new SciArray(dimensions, resultAttributes, resultQuery)
  }


  /**
    * Returns a new array after the computation of the aggregation functions
    * on the required window
    *
    * @param windowSizes
    * @param aggregationCalls
    * @return
    */
  def window(windowSizes:(Int,Int)*)(aggregationCalls: Aggregate*) : SciArray =
  {
    val context = (dimensions, attributes)

    // aggregates evaluation ----------------------
    if(aggregationCalls.isEmpty)
      throw new IllegalOperationSciException("The window statement should have at least one aggregate call")

    val evaluatedCalls = aggregationCalls.map((item:Aggregate) => item.eval(context))

    val duplicatedCalls = evaluatedCalls.map(_._3).diff( evaluatedCalls.map(_._3).distinct )
    if( duplicatedCalls.nonEmpty )
      throw new PropertyAmbiguitySciException("The attributes "+ duplicatedCalls.map("'" + _ + "'").mkString(",") +" already exists, please define distinct name for new attributes")

    // new array construction ---------------------
    var resultAttributes = evaluatedCalls.map((item:(String,DataType,String)) => new Attribute(item._3, item._2)).toList

    // query construction -------------------------
    var resultQuery = "window(\n"+ query.tab() +","
    resultQuery += "\n\t"+ windowSizes.map(item => item._1.toString+", "+item._2.toString).mkString(", ") + ","
    resultQuery += "\n\t" + evaluatedCalls.map(_._1).mkString(",\n\t")
    resultQuery += "\n)"

    // output -------------------------------------
    new SciArray(dimensions, resultAttributes, resultQuery)
  }


  // ------------------------------------------------------------
  // -- DERIVED OPERATORS ---------------------------------------

  // projection prendendo tutti tranne quelli indicati
  // rename di una dimensione
  // rename di un attributo


  // ------------------------------------------------------------
  // -- OVERRIDE ------------------------------------------------

  override def toString() : String = super.toString()

  def description() : String = super.description("")

}


// -------------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------------


/**
  * This class provide the factory methods for arrays in SciDB like
  * the functionalities to import a new array from an external file
  */
object SciArray
{

  /**
    * Returns a new array starting from the list of the array
    * attributes
    *
    * @param name array name to be listed
    * @return the array with the attributes list
    */
  def attributes(name:String) : SciArray =
  {
    // dimensions construction --------------------
    val resultDimensions = List( Dimension("No", 0, None, 100, 0) )

    // attributes construction --------------------
    val resultAttributes = List(
      Attribute("name", STRING, false),
      Attribute("type_id", STRING, false),
      Attribute("nullable", BOOL, false)
    )

    // query construction -------------------------
    var resultQuery = "attributes("+ name +")"

    // output -------------------------------------
    new SciArray(resultDimensions, resultAttributes, resultQuery)
  }

  /**
    * Returns a new array importing it from an external file
    *
    * @param path complete path of the source file
    * @param dimensions dimensions expected from the input
    * @param attributes attributes expected from the input
    * @param instance instance for performing the input
    * @param format input data file format
    * @param max_errors maximum errors permitted before fail
    * @param strict strict option
    * @return the array imported from the file
    * @see <a href="http://www.paradigm4.com/HTMLmanual/15.7/scidb_ug/inputOperator.html">Input specification</a>
    */
  def input(path:String,
            dimensions:List[Dimension],
            attributes:List[Attribute],
            instance:Int = -2,
            format:String = "text",
            max_errors:Int = 0,
            strict:Boolean = false)
    : SciArray =
  {
    // arguments validation -----------------------
    format match
    {
      case ("csv"|"opaque"|"text"|"tsv") => true
      case _ => throw new UnsupportedOperationSciException("Unsupported input format, '"+ format +"' is not valid")
    }

    if(max_errors < 0)
      throw new SyntaxErrorSciException("The argument 'max_error' should be zero or positive")

    val shadow = "INPUT_SHADOW_"+
      System.currentTimeMillis / 1000 +"_"+
      Paths.get(path).getFileName.toString.replace('.','_')

    // query construction -------------------------
    var resultQuery = "input("
    resultQuery += "\n\t<"+ attributes.map(_.toString()).mkString(", ") +">"
    resultQuery += "\n\t\t["+ dimensions.map(_.toString()).mkString(", ") +"],"
    resultQuery += "\n\t'"+ path +"',"
    resultQuery += "\n\t"+ instance + ", '"+ format +"'"

    if( max_errors != 0 || strict )
      resultQuery += ", "+ max_errors + ", '"+ shadow +"', " + strict

    resultQuery += "\n)"

    // output -------------------------------------
    new SciArray(dimensions, attributes, resultQuery)
  }

}