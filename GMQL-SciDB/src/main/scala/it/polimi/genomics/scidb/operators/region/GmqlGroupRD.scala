package it.polimi.genomics.scidb.operators.region

import it.polimi.genomics.core.DataStructures.RegionAggregate.R2RAggregator
import it.polimi.genomics.scidb.evaluators.AggregateEvaluator
import it.polimi.genomics.scidb.libraries.gmql4scidb
import it.polimi.genomics.scidb.operators.{GDS, GmqlRegionOperator}
import it.polimi.genomics.scidb.utility.DebugUtils
import it.polimi.genomics.scidbapi.SciArray
import it.polimi.genomics.scidbapi.aggregate.AGGR
import it.polimi.genomics.scidbapi.expression._
import it.polimi.genomics.scidbapi.schema.Attribute
import it.polimi.genomics.scidbapi.script.SciScript

/**
  * Created by Cattani Simone on 15/04/16.
  * Email: simone.cattani@mail.polimi.it
  *
  */
class GmqlGroupRD(source : GmqlRegionOperator,
                  group_keys : List[Int],
                  aggregates : List[_<:R2RAggregator])
  extends GmqlRegionOperator
{
  /**
    * Apply the it.polimi.genomics.scidb.test.operator producing the result, it requires the
    * child nodes results if necessary
    *
    * @param script Context script
    */
  override def apply(script: SciScript): SciArray =
  {
    val SOURCE = source.compute(script)

    // ------------------------------------------------
    // ------------------------------------------------

    val GROUPED = SOURCE
      .apply((GDS.gid_A.e,
        LIB(gmql4scidb.hash,
          OP(V("group"), "+",
            group_keys
              .map(index => OP(V("$$$"), "+", C_STRING(SOURCE.getAttributes()(index).e)))
              .reduce((e1,e2) => OP(e1, "+", e2))
          )
        )
      ))
      .redimension(GDS.regions_dimensions_D ::: List(GDS.gid_D), SOURCE.getAttributes())

    // ------------------------------------------------
    // ------------------------------------------------

    /*val aggregators = aggregates.map(item =>
      AGGR(
        AggregateEvaluator(item.function_identifier),
        SOURCE.getAttributes()(item.input_index).e,
        (if(item.output_name.isDefined) A(item.output_name.get) else null)
      )
    )

    val aggregatesAtrributes = aggregators.map(item => {
      val (_, datatype, name) = item.eval((SOURCE.getDimensions(), SOURCE.getAttributes()))
      Attribute(name, datatype)
    })*/

    val operations = aggregates.map(item =>
      AggregateEvaluator(
        item.function_identifier,
        SOURCE.getAttributes()(item.input_index).e,
        (if(item.output_name.isDefined) A(item.output_name.get)
        else A(item.function_identifier+"_"+SOURCE.getAttributes()(item.input_index).name))
      )
    )

    val aggregators = operations.map(_._1).reduce((l1,l2) => l1:::l2)
    val aggregatorsAtrributes = aggregators.map(item => {
      val (_, datatype, name) = item.eval((SOURCE.getDimensions(), SOURCE.getAttributes()))
      Attribute(name, datatype)
    })

    val expressions = operations.map(e => if(e._2.isDefined) List(e._2.get) else List()).reduce((l1,l2) => l1:::l2)
    val expressionsAttributes = expressions.map(item => {
      val (_, datatype) = item._2.eval((SOURCE.getDimensions(), SOURCE.getAttributes():::aggregatorsAtrributes))
      Attribute(item._1.attribute, datatype)
    })

    val requiredAttributes = aggregates.map(item =>
      (if(item.output_name.isDefined) A(item.output_name.get)
      else A(item.function_identifier+"_"+SOURCE.getAttributes()(item.input_index).name))
    )
    val resultAttributes = (aggregatorsAtrributes ::: expressionsAttributes)
      .filter(attr => requiredAttributes.contains(attr.e))

    // ------------------------------------------------
    // ------------------------------------------------

    val AGGREGATED = GROUPED
      .redimension(
        List(
          GDS.sid_D,
          GDS.chr_D,
          GDS.left_D,
          GDS.right_D,
          GDS.strand_D,
          GDS.gid_D
        ),
        group_keys.map(SOURCE.getAttributes()(_)) ::: aggregatorsAtrributes,
        false, aggregators:_*
      )

    val APPLYED = if(expressions.isEmpty) AGGREGATED else AGGREGATED
      .apply(expressions:_*)

    // ------------------------------------------------
    // ------------------------------------------------

    val RESULT = APPLYED
      .apply((GDS.enumeration_D.toAttribute().e, GDS.gid_D.e))
      .redimension(
        GDS.regions_dimensions_D,
        group_keys.map(SOURCE.getAttributes()(_)) ::: resultAttributes
      )

    // ------------------------------------------------
    // ------------------------------------------------

    RESULT
  }

}
