package it.polimi.genomics.scidb.operators.meta

import it.polimi.genomics.core.DataStructures.MetadataCondition.{MetadataCondition}
import it.polimi.genomics.core.DataStructures.MetadataCondition.META_OP._
import it.polimi.genomics.scidb.GmqlSciConfig
import it.polimi.genomics.scidb.evaluators.MetaConditionsEvaluator
import it.polimi.genomics.scidb.exception.UnsupportedOperationGmqlSciException
import it.polimi.genomics.scidb.operators.{GDS, GmqlMetaOperator}
import it.polimi.genomics.scidbapi.SciArray
import it.polimi.genomics.scidbapi.expression._
import it.polimi.genomics.scidbapi.schema.DataType._
import it.polimi.genomics.scidbapi.schema.Attribute
import it.polimi.genomics.scidbapi.script.SciScript


/**
  * This it.polimi.genomics.scidb.test.operator implements the selection on metadatas
  *
  * @param child child node
  * @param conditions selection conditions
  */
class GmqlSelectMD(source:GmqlMetaOperator, conditions:MetadataCondition)
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
    val SOURCE = source.compute(script)

    // ------------------------------------------------
    // ------------------------------------------------

    if(GmqlSciConfig.select_bypass)
      return SOURCE

    // ------------------------------------------------
    // Samples ids filter definition ------------------

    val SIDS = MetaConditionsEvaluator(SOURCE, conditions)

    // ------------------------------------------------
    // Samples ids filter application -----------------

    val SELECTED = SOURCE
      .cross_join(SIDS, "SOURCE", "FILTER")((GDS.sid_D.alias("SOURCE").e, GDS.sid_D.alias("FILTER").e))
      .project(GDS.meta_attributes_A.map(_.e) : _*)

    // ------------------------------------------------

    SELECTED
  }

}
