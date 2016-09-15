package it.polimi.genomics.scidb.operators.meta

import it.polimi.genomics.core.DataStructures.RegionAggregate.R2MAggregator
import it.polimi.genomics.scidb.evaluators.AggregateEvaluator
import it.polimi.genomics.scidb.libraries.gmql4scidb
import it.polimi.genomics.scidb.operators.{GDS, GmqlRegionOperator, GmqlMetaOperator}
import it.polimi.genomics.scidbapi.SciArray
import it.polimi.genomics.scidbapi.aggregate.{AGGR, AGGR_SUM}
import it.polimi.genomics.scidbapi.expression._
import it.polimi.genomics.scidbapi.schema.{Dimension, Attribute}
import it.polimi.genomics.scidbapi.schema.DataType._
import it.polimi.genomics.scidbapi.script.SciScript

/**
  * Created by Cattani Simone on 15/04/16.
  * Email: simone.cattani@mail.polimi.it
  *
  */
class GmqlGroupMD(metadata : GmqlMetaOperator,
                  regions : GmqlRegionOperator,
                  group_keys : List[String],
                  group_name : String,
                  aggregates : List[_<:R2MAggregator])
  extends GmqlMetaOperator
{
  /**
    * Apply the it.polimi.genomics.scidb.test.operator producing the result, it requires the
    * child nodes results if necessary
    *
    * @param script Context script
    */
  override def apply(script: SciScript): SciArray =
  {
    val METADATA = metadata.compute(script)
    val REGIONS = regions.compute(script)

    // ------------------------------------------------
    // ------------------------------------------------

    val key_A = Attribute("key", STRING)
    val sort_D = Dimension("i", 0, None, Dimension.DEFAULT_CHUNK_LENGTH, Dimension.DEFAULT_OVERLAP)

    // grouping ---------------------------------------

    val ATTRIBUTES = group_keys.zipWithIndex.map(item =>

      METADATA
        .filter(AND(OP(GDS.nid1_D.e, "=", LIB(gmql4scidb.hash, V(item._1))), OP(GDS.nid2_D.e, "=", LIB(gmql4scidb.hash, OP(V(item._1), "+", C_STRING(V("0")))))))
        .apply((key_A.e, OP(GDS.value_A.e, "+", V("$$$"))))
        .redimension(List(GDS.sid_D), List(key_A.rename("aggr_key")), false, AGGR_SUM(key_A.e, key_A.rename("aggr_key").e))
        .apply((key_A.e, LIB(gmql4scidb.key_sort, key_A.rename("aggr_key").e)))
        .project(key_A.e)
        .cast(List(GDS.sid_D), List(key_A.rename("key_" + item._2)))

    ).reduce((A1,A2) =>
      A1.cross_join(A2, "F", "S")((GDS.sid_D.alias("F").e, GDS.sid_D.alias("S").e))
    )

    // index ------------------------------------------

    val KEYS = ATTRIBUTES
      .apply(( key_A.e, (0 to (group_keys.size-1)).map(i => key_A.rename("key_"+i).e:Expr).reduce((a1,a2) => OP(a1, "+", a2)) ))

    val INDEXES = KEYS
      .project(key_A.e)
      .sort()().uniq()
      .cast(List(sort_D), List(Attribute(key_A.name, STRING, false)))

    val GROUPED = KEYS
      .index_lookup(INDEXES, key_A.e, GDS.gid_A.e)

    // ------------------------------------------------
    // ------------------------------------------------

    val GROUPDATA = GROUPED
      .apply(
        (GDS.value_A.e, C_STRING(GDS.gid_A.e)),
        (GDS.name_A.e, V(group_name)))
      .apply(
        (A(GDS.nid1_D.name), LIB(gmql4scidb.hash, GDS.name_A.e)),
        (A(GDS.nid2_D.name), LIB(gmql4scidb.hash, OP(GDS.name_A.e,"+",C_STRING(V("0"))))),
        (A(GDS.vid1_D.name), LIB(gmql4scidb.hash, GDS.value_A.e)),
        (A(GDS.vid2_D.name), LIB(gmql4scidb.hash, OP(GDS.value_A.e,"+",C_STRING(V("0"))))))
      .redimension(GDS.meta_dimensions_D, GDS.meta_attributes_A)

    // ------------------------------------------------
    // ------------------------------------------------

    val GROUPED_REGIONS = REGIONS
      .cross_join(
        GROUPED
          .apply((GDS.null1, V(1)))
          .redimension(GDS.metagroup_dimensions_D, GDS.metagroup_attributes_A),
        "REGIONS", "GROUP"
      )((GDS.sid_D.alias("REGIONS").e, GDS.sid_D.alias("GROUP").e))

    // ------------------------------------------------
    // ------------------------------------------------

    //DebugUtils.exec(GROUPED_REGIONS, script)

    val GROUP_AGGREGATES = aggregates.map(item => {

      val (aggregators, expression) = AggregateEvaluator(
        item.function_identifier,
        GROUPED_REGIONS.getAttributes()(item.input_index).e,
        GDS.value_A.label("tmp").e
      )

      val AGGREGATED_REGIONS = GROUPED_REGIONS
        .aggregate(aggregators:_*)(GDS.gid_D.e)

      val APPLIED_REGIONS = if(expression.isEmpty) AGGREGATED_REGIONS else AGGREGATED_REGIONS
        .apply(expression.get)

      APPLIED_REGIONS
        .apply(
          (GDS.name_A.e, V(item.output_attribute_name)),
          (GDS.value_A.e, C_STRING(GDS.value_A.label("tmp").e))
        )
        .apply(
          (A(GDS.nid1_D.name), LIB(gmql4scidb.hash, GDS.name_A.e)),
          (A(GDS.nid2_D.name), LIB(gmql4scidb.hash, OP(GDS.name_A.e, "+", C_STRING(V("0"))))),
          (A(GDS.vid1_D.name), LIB(gmql4scidb.hash, GDS.value_A.e)),
          (A(GDS.vid2_D.name), LIB(gmql4scidb.hash, OP(GDS.value_A.e, "+", C_STRING(V("0")))))
        )
        .redimension(List(
          GDS.nid1_D, GDS.nid2_D, GDS.vid1_D, GDS.vid2_D, GDS.gid_D
        ), GDS.meta_attributes_A)

    }).reduce((a1,a2) => a1.merge(a2))

    // ------------------------------------------------

    val AGGREGATES = GROUP_AGGREGATES
      .cross_join(
        GROUPED
          .apply((GDS.null1, V(1)))
          .redimension(GDS.metagroup_dimensions_D, GDS.metagroup_attributes_A),
        "AGGREGATE", "GROUP"
      )((GDS.gid_D.alias("AGGREGATE").e, GDS.gid_D.alias("GROUP").e))
      .redimension(GDS.meta_dimensions_D, GDS.meta_attributes_A)

    // ------------------------------------------------
    // ------------------------------------------------

    val RESULT = METADATA
      .merge(GROUPDATA)
      .merge(AGGREGATES)

    // ------------------------------------------------
    // ------------------------------------------------

    RESULT
  }

}
