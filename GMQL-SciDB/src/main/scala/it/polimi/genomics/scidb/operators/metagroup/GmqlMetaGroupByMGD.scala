package it.polimi.genomics.scidb.operators.metagroup

import it.polimi.genomics.core.DataStructures.MetaGroupByCondition.MetaGroupByCondition
import it.polimi.genomics.scidb.libraries.gmql4scidb
import it.polimi.genomics.scidb.operators.{GDS, GmqlMetaOperator, GmqlMetaGroupOperator}
import it.polimi.genomics.scidbapi.SciArray
import it.polimi.genomics.scidbapi.aggregate.{AGGR_SUM}
import it.polimi.genomics.scidbapi.expression._
import it.polimi.genomics.scidbapi.schema.{Attribute, Dimension}
import it.polimi.genomics.scidbapi.schema.DataType._
import it.polimi.genomics.scidbapi.script.SciScript


/**
  * This it.polimi.genomics.scidb.test.operator implements the group by operation
  * that divides into groups the samples of a dataset
  *
  * @param source source dataset
  * @param condition grouping conditions
  */
class GmqlMetaGroupByMGD(source : GmqlMetaOperator,
                         condition : MetaGroupByCondition)
  extends GmqlMetaGroupOperator
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

    val key_A = Attribute("key", STRING)
    val sort_D = Dimension("i", 0, None, Dimension.DEFAULT_CHUNK_LENGTH, Dimension.DEFAULT_OVERLAP)

    // ------------------------------------------------
    // grouping ---------------------------------------

    val ATTRIBUTES = condition.attributes.zipWithIndex.map(item =>

      SOURCE
        .filter( AND(OP(GDS.nid1_D.e, "=", LIB(gmql4scidb.hash, V(item._1))), OP(GDS.nid2_D.e, "=", LIB(gmql4scidb.hash, OP(V(item._1),"+",C_STRING(V("0")))))) )
        .apply((key_A.e, OP(GDS.value_A.e, "+", V("$$$"))))
        .redimension(List(GDS.sid_D), List(key_A.rename("aggr_key")), false, AGGR_SUM(key_A.e, key_A.rename("aggr_key").e))
        .apply((key_A.e, LIB(gmql4scidb.key_sort, key_A.rename("aggr_key").e)))
        .project(key_A.e)
        .cast(List(GDS.sid_D), List(key_A.rename("key_"+item._2)))

    ).reduce((A1,A2) =>
      A1.cross_join(A2, "F", "S")((GDS.sid_D.alias("F").e, GDS.sid_D.alias("S").e))
    )

    // ------------------------------------------------
    // index ------------------------------------------

    val KEYS = ATTRIBUTES
      .apply(( key_A.e, (0 to (condition.attributes.size-1)).map(i => key_A.rename("key_"+i).e:Expr).reduce((a1,a2) => OP(a1, "+", a2)) ))

    val INDEXES = KEYS
      .project(key_A.e)
      .sort()().uniq()
      .cast(List(sort_D), List(Attribute(key_A.name, STRING, false)))

    val GROUPED = KEYS
      .index_lookup(INDEXES, key_A.e, GDS.gid_A.e)
      .apply((GDS.null1, V(1)))
      .redimension(GDS.metagroup_dimensions_D, GDS.metagroup_attributes_A)

    // ------------------------------------------------

    GROUPED
  }

}
