package it.polimi.genomics.scidb.operators.meta

import it.polimi.genomics.core.DataStructures.RegionAggregate.{R2MAggregator, R2RAggregator}
import it.polimi.genomics.scidb.evaluators.AggregateEvaluator
import it.polimi.genomics.scidb.libraries.gmql4scidb
import it.polimi.genomics.scidb.operators.{GmqlRegionOperator, GDS, GmqlMetaOperator}
import it.polimi.genomics.scidb.utility.DebugUtils
import it.polimi.genomics.scidbapi.SciArray
import it.polimi.genomics.scidbapi.aggregate.AGGR
import it.polimi.genomics.scidbapi.expression._
import it.polimi.genomics.scidbapi.script.SciScript

/**
  * Created by Cattani Simone on 14/04/16.
  * Email: simone.cattani@mail.polimi.it
  *
  */
class GmqlAggregateMD(regions : GmqlRegionOperator,
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
    val REGIONS = regions.compute(script)

    // ------------------------------------------------
    // ------------------------------------------------

    val VALUES = aggregates.map(item => {

      val (aggregators, expression) = AggregateEvaluator(
        item.function_identifier,
        REGIONS.getAttributes()(item.input_index).e,
        GDS.value_A.label("tmp").e
      )

      val AGGREGATED_REGIONS = REGIONS
        .aggregate(aggregators:_*)(GDS.sid_D.e)

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
        .redimension(GDS.meta_dimensions_D, GDS.meta_attributes_A)

    }).reduce((a1,a2) => a1.merge(a2))

    // ------------------------------------------------
    // ------------------------------------------------

    VALUES
  }

}
