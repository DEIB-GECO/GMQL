package it.polimi.genomics.scidb.operators.region

import it.polimi.genomics.core.DataStructures.CoverParameters.CoverFlag.CoverFlag
import it.polimi.genomics.core.DataStructures.CoverParameters.{CoverFlag, ANY, N, CoverParam}
import it.polimi.genomics.core.DataStructures.RegionAggregate.{RegionsToRegion, R2RAggregator}
import it.polimi.genomics.core.GValue
import it.polimi.genomics.core.ParsingType._
import it.polimi.genomics.scidb.evaluators.AggregateEvaluator
import it.polimi.genomics.scidb.exception.UnsupportedOperationGmqlSciException
import it.polimi.genomics.scidb.operators.{GDS, GmqlMetaGroupOperator, GmqlRegionOperator}
import it.polimi.genomics.scidb.utility.DebugUtils
import it.polimi.genomics.scidbapi.SciArray
import it.polimi.genomics.scidbapi.aggregate._
import it.polimi.genomics.scidbapi.expression._
import it.polimi.genomics.scidbapi.schema.{Attribute}
import it.polimi.genomics.scidbapi.schema.DataType._
import it.polimi.genomics.scidbapi.script.{SciOperation, SciScript}


/**
  *
  *
  * @param source
  * @param groups
  * @param method
  * @param min
  * @param max
  * @param aggregates
  */
class GmqlGenometricCoverRD(source : GmqlRegionOperator,
                            groups : Option[GmqlMetaGroupOperator],
                            method : CoverFlag,
                            min : CoverParam, max : CoverParam,
                            aggregates : List[_<:R2RAggregator])
  extends GmqlRegionOperator
{
  // ------------------------------------------------------------
  // ------------------------------------------------------------

  val base_D = GDS.left_D.rename("base")

  val left_count_A = Attribute("left_count", INT64)
  val right_count_A = Attribute("right_count", INT64)
  val hash_count_A = Attribute("hash_count", INT64)
  val count_A = hash_count_A.rename("count")

  val region_start_A = Attribute("region_start", INT64)
  val region_stop_A = Attribute("region_stop", INT64)

  val left_end_A = GDS.left_A.rename("left_end")
  val right_end_A = GDS.right_A.rename("right_end")

  var store_internals = true

  // ------------------------------------------------------------
  // ------------------------------------------------------------

  /**
    * Apply the it.polimi.genomics.scidb.test.operator producing the result, it requires the
    * child nodes results if necessary
    *
    * @param script Context script
    */
  override def apply(script: SciScript): SciArray =
  {
    val SOURCE = source.compute(script)
    val GROUPS = if(groups.isEmpty) None else Some(groups.get.compute(script))

    // ------------------------------------------------
    // ------------------------------------------------

    var PREPARED = prepare(SOURCE, GROUPS)

    if(store_internals){
      val PREPARED_STORED = PREPARED.store(script.getTmpName +"_"+ this.getClass.getSimpleName +"_Internal001")

      script.addStatement(PREPARED_STORED)
      script.addQueueStatement(SciOperation("remove", PREPARED_STORED.getAnchor()))

      PREPARED = PREPARED_STORED.reference()
    }

    // ------------------------------------------------
    // ------------------------------------------------

    // ------------------------------------------------
    // ------------------------------------------------

    var HASHMAP = hash(PREPARED)

    if(store_internals){
      val HASHMAP_STORED = HASHMAP.store(script.getTmpName +"_"+ this.getClass.getSimpleName +"_Internal002")

      script.addStatement(HASHMAP_STORED)
      script.addQueueStatement(SciOperation("remove", HASHMAP_STORED.getAnchor()))

      HASHMAP = HASHMAP_STORED.reference()
    }

    // ------------------------------------------------
    // ------------------------------------------------

    // ------------------------------------------------
    // ------------------------------------------------

    val filter = (min, max) match
    {
      case (low:N, high:ANY) => OP(count_A.e, ">=", V(low.n))
      case (low:N, high:N) => AND(OP(count_A.e, ">=", V(low.n)), OP(count_A.e, "<=", V(high.n)))
      case _ => throw new UnsupportedOperationGmqlSciException("Cover parameters not valid")
    }

    val HISTOGRAM = histogram(HASHMAP)
      .filter(filter)

    // ------------------------------------------------
    // ------------------------------------------------

    // ------------------------------------------------
    // ------------------------------------------------

    val METHOD = method match
    {
      case CoverFlag.HISTOGRAM => HISTOGRAM
      case CoverFlag.COVER | CoverFlag.FLAT => merge(HISTOGRAM)
      //case CoverFlag.SUMMIT => summit(HISTOGRAM)
    }

    // ------------------------------------------------

    val PREPARED_EXT = method match
    {
      case CoverFlag.FLAT => PREPARED
        .apply(
          (left_end_A.e, GDS.left_D.e),
          (right_end_A.e, GDS.right_D.e)
        )
      case _ => PREPARED
    }

    // ------------------------------------------------

    val aggregates_EXT = method match
    {
      case CoverFlag.FLAT => {

        var leftEnd = new FakeR2R
        leftEnd.function_identifier = "min"
        leftEnd.input_index = PREPARED.getAttributes().size
        leftEnd.output_name = Some(region_start_A.name)

        var rightEnd = new FakeR2R
        rightEnd.function_identifier = "max"
        rightEnd.input_index = PREPARED.getAttributes().size+1
        rightEnd.output_name = Some(region_stop_A.name)

        List(leftEnd, rightEnd) ::: aggregates
      }
      case _ => aggregates
    }

    // ------------------------------------------------
    // ------------------------------------------------

    // ------------------------------------------------
    // ------------------------------------------------

    val COVERED = cover(METHOD, PREPARED_EXT, aggregates_EXT)

    // ------------------------------------------------
    // ------------------------------------------------

    // ------------------------------------------------
    // ------------------------------------------------

    val RESULT = method match
    {
      case CoverFlag.FLAT => flat(COVERED)
      case _ => COVERED
    }

    // ------------------------------------------------
    // ------------------------------------------------

    DebugUtils.exec(RESULT, script)

    SOURCE
  }


  // ------------------------------------------------------------
  // ------------------------------------------------------------

  /**
    * Prepares regions for the computation of the cover
    *
    * @param SOURCE
    * @return
    */
  def prepare(SOURCE : SciArray,
              GROUPS : Option[SciArray])
    : SciArray =
  {
    val GROUPED = GROUPS match
    {
      case None => SOURCE
        .apply((GDS.gid_A.e, V(1)))
        .redimension(GDS.regions_dimensions_D ::: List(GDS.gid_D), SOURCE.getAttributes())

      case Some(groups) => {
        SOURCE
          .cross_join(groups, "SOURCE", "GROUPS")((GDS.sid_D.alias("SOURCE").e, GDS.sid_D.alias("GROUPS").e))
          .redimension(GDS.regions_dimensions_D ::: List(GDS.gid_D), SOURCE.getAttributes())  }
    }

    // ------------------------------------------------
    // ------------------------------------------------

    val PREPARED = GROUPED
      .filter(OP(GDS.strand_D.e, "<>", V(1)))
      .merge(
        GROUPED
          .filter(OP(GDS.strand_D.e, "=", V(1)))
          .apply((A("new_strand"), V(0)))
          .redimension(
            List(GDS.sid_D, GDS.chr_D, GDS.left_D, GDS.right_D, GDS.strand_D.rename("new_strand"), GDS.enumeration_D, GDS.gid_D),
            GROUPED.getAttributes()
          )
          .cast(GDS.regions_dimensions_D ::: List(GDS.gid_D), GROUPED.getAttributes())
      )
      .merge(
        GROUPED
          .filter(OP(GDS.strand_D.e, "=", V(1)))
          .apply((A("new_strand"), V(2)))
          .redimension(
            List(GDS.sid_D, GDS.chr_D, GDS.left_D, GDS.right_D, GDS.strand_D.rename("new_strand"), GDS.enumeration_D, GDS.gid_D),
            GROUPED.getAttributes()
          )
          .cast(GDS.regions_dimensions_D ::: List(GDS.gid_D), GROUPED.getAttributes())
      )

    // ------------------------------------------------
    // ------------------------------------------------

    PREPARED
  }


  // ------------------------------------------------------------
  // ------------------------------------------------------------

  /**
    * Computes the hash map for the source
    *
    * @param PREPARED
    * @return
    */
  def hash(PREPARED : SciArray)
    : SciArray =
  {
    val LEFT = PREPARED
      .aggregate(AGGR_COUNT(A("*"), A("count")))(GDS.gid_D.e, GDS.chr_D.e, GDS.strand_D.e, GDS.left_D.e)
      .apply((left_count_A.e, C_INT64(A("count"))))
      .project(left_count_A.e)
      .cast(List(GDS.gid_D, GDS.chr_D, GDS.strand_D, base_D), List(left_count_A))

    val RIGHT = PREPARED
      .aggregate(AGGR_COUNT(A("*"), A("count")))(GDS.gid_D.e, GDS.chr_D.e, GDS.strand_D.e, GDS.right_D.e)
      .apply((right_count_A.e, C_INT64(A("count"))))
      .project(right_count_A.e)
      .cast(List(GDS.gid_D, GDS.chr_D, GDS.strand_D, base_D), List(right_count_A))

    // ------------------------------------------------
    // ------------------------------------------------

    val LEFT_EXP = LEFT
      .merge(RIGHT.apply((left_count_A.e, OP(right_count_A.e, "*", V(0)))).project(left_count_A.e))

    val RIGHT_EXP = RIGHT
      .merge(LEFT.apply((right_count_A.e, OP(left_count_A.e, "*", V(0)))).project(right_count_A.e))

    // ------------------------------------------------
    // ------------------------------------------------

    val HASHMAP = LEFT_EXP
      .cross_join(
        RIGHT_EXP,
        "LEFT", "RIGHT"
      )(
        (GDS.gid_D.alias("LEFT").e, GDS.gid_D.alias("RIGHT").e),
        (GDS.chr_D.alias("LEFT").e, GDS.chr_D.alias("RIGHT").e),
        (GDS.strand_D.alias("LEFT").e, GDS.strand_D.alias("RIGHT").e),
        (base_D.alias("LEFT").e, base_D.alias("RIGHT").e)
      )
      .apply((hash_count_A.e, OP(left_count_A.e, "-", right_count_A.e)))

    // ------------------------------------------------
    // ------------------------------------------------

    HASHMAP
  }


  // ------------------------------------------------------------
  // ------------------------------------------------------------

  /**
    * Compute the histogram result
    *
    * @param HASH
    * @return
    */
  def histogram(HASH : SciArray)
    : SciArray =
  {
    // ------------------------------------------------
    // ------------------------------------------------

    val COUNT = HASH
      .cumulate(AGGR_SUM(hash_count_A.e, A("tmp")))(base_D.e)
      .apply((count_A.e, A("tmp")))
      .project(count_A.e)

    val REGIONS = HASH
      .apply((GDS.null1_A.e,V(1)))
      .cumulate(AGGR_SUM(GDS.null1_A.e, A("id")))(base_D.e)
      .apply((region_start_A.label("tmp").e, base_D.e))
      .apply((GDS.enumeration_D.toAttribute().e, C_INT64(A("id"))))
      .redimension(List(GDS.gid_D, GDS.chr_D, GDS.strand_D, GDS.enumeration_D), List(region_start_A.label("tmp")))

      .window((0,0),(0,0),(0,0),(0,1))(
        AGGR_MIN(region_start_A.label("tmp").e, region_start_A.e),
        AGGR_MAX(region_start_A.label("tmp").e, region_stop_A.e)
      )
      .filter(OP(region_start_A.e, "<>", region_stop_A.e))

      .cast(
        List(GDS.sid_D, GDS.chr_D, GDS.strand_D, GDS.enumeration_D),
        List(GDS.left_A, GDS.right_A)
      )
      .redimension(
        List(GDS.sid_D, GDS.chr_D, GDS.left_D, GDS.strand_D, GDS.enumeration_D),
        List(GDS.right_A)
      )

      .cross_join(
        COUNT, "REGIONS", "COUNT"
      )(
        (GDS.sid_D.alias("REGIONS").e, GDS.gid_D.alias("COUNT").e),
        (GDS.chr_D.alias("REGIONS").e, GDS.chr_D.alias("COUNT").e),
        (GDS.left_D.alias("REGIONS").e, base_D.alias("COUNT").e),
        (GDS.strand_D.alias("REGIONS").e, GDS.strand_D.alias("COUNT").e)
      )
      .redimension(
        List(GDS.sid_D, GDS.chr_D, GDS.left_D, GDS.right_D, GDS.strand_D, GDS.enumeration_D),
        List(count_A)
      )

    // ------------------------------------------------
    // ------------------------------------------------

    REGIONS
  }


  // ------------------------------------------------------------
  // ------------------------------------------------------------

  /**
    * Applies the aggregation functions
    *
    * @param REFERENCE
    * @param DATA
    * @param aggregates
    * @return
    */
  def cover(REFERENCE : SciArray,
            PREPARED : SciArray,
            aggregates : List[_<:R2RAggregator])
    : SciArray =
  {
    var DATA = PREPARED
      .redimension(
        List(GDS.chr_D, GDS.strand_D, GDS.gid_D, GDS.sid_D, GDS.enumeration_D),
        List(GDS.left_D.toAttribute(false), GDS.right_D.toAttribute(false)) ::: PREPARED.getAttributes(),
        true)

    DATA = DATA
      .cast(DATA.getDimensions().map(_.label("REGION")), DATA.getAttributes().map(_.label("REGION")))

    // ------------------------------------------------
    // ------------------------------------------------

    val INTERSECTED = REFERENCE
      .cross_join(
        DATA, "REFERENCE", "DATA"
      )(
        (GDS.sid_D.alias("REFERENCE").e, GDS.gid_D.label("REGION").alias("DATA").e),
        (GDS.chr_D.alias("REFERENCE").e, GDS.chr_D.label("REGION").alias("DATA").e),
        (GDS.strand_D.alias("REFERENCE").e, GDS.strand_D.label("REGION").alias("DATA").e)
      )
      .filter(
        AND(
          OP(GDS.left_D.toAttribute(false).label("REGION").e, "<", GDS.right_D.e),
          OP(GDS.right_D.toAttribute(false).label("REGION").e, ">", GDS.left_D.e) )
      )

    // ------------------------------------------------
    // ------------------------------------------------

    val operations = aggregates.map(item =>
      AggregateEvaluator(
        item.function_identifier,
        PREPARED.getAttributes()(item.input_index).label("REGION").e,
        (if(item.output_name.isDefined) A(item.output_name.get)
        else A(item.function_identifier+"_"+PREPARED.getAttributes()(item.input_index).label("REGION").name))
      )
    )

    val aggregators = operations.map(_._1).reduce((l1,l2) => l1:::l2)
    val aggregatorsAtrributes = aggregators.map(item => {
      val (_, datatype, name) = item.eval((INTERSECTED.getDimensions(), INTERSECTED.getAttributes()))
      Attribute(name, datatype)
    })

    val expressions = operations.map(e => if(e._2.isDefined) List(e._2.get) else List()).reduce((l1,l2) => l1:::l2)
    val expressionsAttributes = expressions.map(item => {
      val (_, datatype) = item._2.eval((INTERSECTED.getDimensions(), INTERSECTED.getAttributes():::aggregatorsAtrributes))
      Attribute(item._1.attribute, datatype)
    })

    val requiredAttributes = aggregates.map(item =>
      (if(item.output_name.isDefined) A(item.output_name.get)
      else A(item.function_identifier+"_"+PREPARED.getAttributes()(item.input_index).label("REGION").name))
    )
    val resultAttributes = (aggregatorsAtrributes ::: expressionsAttributes)
      .filter(attr => requiredAttributes.contains(attr.e))

    // ------------------------------------------------
    // ------------------------------------------------

    val AGGREGATED = INTERSECTED
      .redimension(
        List(
          GDS.sid_D,
          GDS.chr_D,
          GDS.left_D,
          GDS.right_D,
          GDS.strand_D,
          GDS.enumeration_D
        ),
        aggregatorsAtrributes,
        false,
        aggregators:_*
      )

    val APPLYED = if(expressions.isEmpty) AGGREGATED else AGGREGATED
      .apply(expressions:_*)

    val RESULT = APPLYED
      .cast(GDS.regions_dimensions_D, aggregatorsAtrributes ::: expressionsAttributes)
      .project(resultAttributes.map(_.e):_*)

    // ------------------------------------------------
    // ------------------------------------------------

    RESULT
  }

  // ------------------------------------------------------------
  // ------------------------------------------------------------

  /**
    * Merge the regions
    *
    * @param HISTOGRAM
    * @return
    */
  def merge(HISTOGRAM : SciArray)
    : SciArray =
  {
    // ------------------------------------------------
    // ------------------------------------------------

    val WINDOW = HISTOGRAM
      .redimension(
        List(GDS.sid_D, GDS.chr_D, GDS.strand_D, GDS.enumeration_D),
        List(GDS.left_D.toAttribute(false), GDS.right_D.toAttribute(false)),
        true
      )
      .window((0,0),(0,0),(0,0),(1,1))(
        (AGGR_MIN(GDS.left_D.toAttribute(false).e, region_start_A.label("prev").e)),
        (AGGR_MIN(GDS.right_D.toAttribute(false).e, region_stop_A.label("prev").e)),
        (AGGR_MAX(GDS.left_D.toAttribute(false).e, region_start_A.label("next").e)),
        (AGGR_MAX(GDS.right_D.toAttribute(false).e, region_stop_A.label("next").e))
      )

    // ------------------------------------------------
    // ------------------------------------------------

    val MERGED = HISTOGRAM
      .cross_join(WINDOW, "HISTOGRAM", "WINDOW")(
        (GDS.sid_D.alias("HISTOGRAM").e, GDS.sid_D.alias("WINDOW").e),
        (GDS.chr_D.alias("HISTOGRAM").e, GDS.chr_D.alias("WINDOW").e),
        (GDS.strand_D.alias("HISTOGRAM").e, GDS.strand_D.alias("WINDOW").e),
        (GDS.enumeration_D.alias("HISTOGRAM").e, GDS.enumeration_D.alias("WINDOW").e)
      )
      .apply(
        (A("is_first"), OP(region_stop_A.label("prev").e, "<>", GDS.left_D.e)),
        (A("is_last"), OP(region_start_A.label("next").e, "<>", GDS.right_D.e))
      )
      .filter(
        NOT(AND(
          OP(A("is_first"), "=", V(false)),
          OP(A("is_last"), "=", V(false))
        ))
      )

    // ------------------------------------------------
    // ------------------------------------------------

    val SINGLE = MERGED
      .filter(
        AND(
          OP(A("is_first"), "=", V(true)),
          OP(A("is_last"), "=", V(true))
        ))
      .apply((GDS.null1, V(1)))
      .project(GDS.null1)
      .apply((count_A.e, V(1)))
      .project(count_A.e)

    // ------------------------------------------------
    // ------------------------------------------------

    val NOTSINGLE = MERGED
      .filter(
        NOT(AND(
          OP(A("is_first"), "=", V(true)),
          OP(A("is_last"), "=", V(true))
        ))
      )
      .project(A("is_first"))

    // ------------------------------------------------

    val REGIONS = NOTSINGLE
      .redimension(
        List(GDS.sid_D, GDS.chr_D, GDS.strand_D, GDS.enumeration_D),
        List(GDS.left_D.toAttribute(false), GDS.right_D.toAttribute(false)),
        true
      )
      .variable_window(
        GDS.enumeration_D.e, 0, 1,
        (AGGR_MIN(GDS.left_D.toAttribute(false).e, region_start_A.label("prev").e)),
        (AGGR_MIN(GDS.right_D.toAttribute(false).e, region_stop_A.label("prev").e)),
        (AGGR_MAX(GDS.left_D.toAttribute(false).e, region_start_A.label("next").e)),
        (AGGR_MAX(GDS.right_D.toAttribute(false).e, region_stop_A.label("next").e))
      )

    // ------------------------------------------------

    val COMPOSED = NOTSINGLE
      .filter(A("is_first"))
      .cross_join(REGIONS, "NOTSINGLE", "REGIONS")(
        (GDS.sid_D.alias("NOTSINGLE").e, GDS.sid_D.alias("REGIONS").e),
        (GDS.chr_D.alias("NOTSINGLE").e, GDS.chr_D.alias("REGIONS").e),
        (GDS.strand_D.alias("NOTSINGLE").e, GDS.strand_D.alias("REGIONS").e),
        (GDS.enumeration_D.alias("NOTSINGLE").e, GDS.enumeration_D.alias("REGIONS").e)
      )
      .apply((GDS.null1, V(1)))
      .redimension(
        List(
          GDS.sid_D, GDS.chr_D, GDS.left_D,
          GDS.right_D.rename(region_stop_A.label("next").name),
          GDS.strand_D, GDS.enumeration_D),
        List(region_start_A.label("prev"))
      )
      .cast(GDS.regions_dimensions_D, List(region_start_A.label("prev")))
      .apply((count_A.e, V(1)))
      .project(count_A.e)

    // ------------------------------------------------
    // ------------------------------------------------

    val RESULT = SINGLE.merge(COMPOSED)

    // ------------------------------------------------
    // ------------------------------------------------

    RESULT
  }

  // ------------------------------------------------------------
  // ------------------------------------------------------------

  /**
    * Builds the flat result
    *
    * @param SOURCE
    * @return
    */
  def flat(SOURCE : SciArray) : SciArray =
  {
    val attributes = SOURCE.getAttributes().tail.tail

    // ------------------------------------------------
    // ------------------------------------------------

    val RESULT = SOURCE
      .redimension(
        List(GDS.sid_D, GDS.chr_D, GDS.strand_D, GDS.enumeration_D),
        SOURCE.getAttributes(),
        true
      )
      .cast(
        List(GDS.sid_D, GDS.chr_D, GDS.strand_D, GDS.enumeration_D),
        List(GDS.left_A, GDS.right_A) ::: attributes
      )
      .redimension(
        GDS.regions_dimensions_D,
        attributes
      )

    // ------------------------------------------------
    // ------------------------------------------------

    RESULT
  }


  // ------------------------------------------------------------
  // ------------------------------------------------------------


  class FakeR2R extends RegionsToRegion {
    override val resType: PARSING_TYPE = INTEGER
    override val funOut: (GValue, (Int, Int)) => GValue = null
    override val index: Int = -1
    override val fun: (List[GValue]) => GValue = null
    override val associative: Boolean = true
  }

}

