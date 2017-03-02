package it.polimi.genomics.scidb.operators.meta

import it.polimi.genomics.core.DataStructures.GroupMDParameters.Direction._
import it.polimi.genomics.core.DataStructures.GroupMDParameters.{TopG, Top, NoTop, TopParameter}
import it.polimi.genomics.scidb.GmqlSciConfig
import it.polimi.genomics.scidb.libraries.gmql4scidb
import it.polimi.genomics.scidb.operators.{GDS, GmqlMetaOperator}
import it.polimi.genomics.scidbapi.SciArray
import it.polimi.genomics.scidbapi.aggregate.{AGGR, AGGR_MAX, AGGR_MIN}
import it.polimi.genomics.scidbapi.expression._
import it.polimi.genomics.scidbapi.schema.{Dimension, Attribute}
import it.polimi.genomics.scidbapi.schema.DataType._
import it.polimi.genomics.scidbapi.script.SciScript
import it.polimi.genomics.scidbapi.SortingKeyword._


/**
  * This it.polimi.genomics.scidb.test.operator implements the metadata ordering
  *
  * @param source source metadata
  * @param ordering ordering rules
  * @param top top parameter
  * @param destination destination attribute name
  */
class GmqlOrderMD(source : GmqlMetaOperator,
                  ordering : List[(String,Direction)],
                  top : TopParameter,
                  destination : String)
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

    val rank_A = Attribute("rank", DOUBLE)
    val order_A = Attribute("order", INT64)
    val group_D = Dimension("group", 0, None, 100, 0)

    // ------------------------------------------------
    // Ranking ----------------------------------------

    val RANKS = ordering.zipWithIndex.map(item =>

      SOURCE
        .filter( AND(OP(GDS.nid1_D.e, "=", LIB(gmql4scidb.hash, V(item._1._1))), OP(GDS.nid2_D.e, "=", LIB(gmql4scidb.hash, OP(V(item._1._1),"+",C_STRING(V("0")))))) )
        .rank(GDS.value_A.e)()
        .redimension(List(GDS.sid_D), List(rank_A), false, AGGR((if(item._1._2 == ASC) "min" else "max"), A("value_rank"), rank_A.e))
        .cast(List(GDS.sid_D), List(rank_A.rename("rank_"+item._2)))

    ).reduce((A1,A2) =>
      A1.cross_join(A2, "F", "S")((GDS.sid_D.alias("F").e, GDS.sid_D.alias("S").e))
    )

    // ------------------------------------------------
    // Sorting ----------------------------------------

    val ranks : List[(A, SortingKeyword)] =
      ordering.zipWithIndex.map(item => (
        A("rank_"+item._2),
        item._1._2 match {
          case ASC => asc
          case DESC => desc
        }
      ))

    val SORTED = RANKS
      .unpack(D("synth"))
      .sort(ranks:_*)()
      .apply((order_A.e, D("n") ))

    // ------------------------------------------------
    // Topping ----------------------------------------

    val TOPPED = top match
    {
      case NoTop() => SORTED

      case Top(k) => SORTED
          .filter(OP(order_A.e, "<", V(k)))

      case TopG(k) => SORTED
        .apply(( group_D.toAttribute().e, LIB(gmql4scidb.hash, ranks.map(a => C_STRING(a._1):Expr).reduce((a1, a2) => OP(a1, "+", a2)))))
        .redimension(List(GDS.sid_D, group_D), List(order_A))
        .rank(order_A.e)(group_D.e)
        .filter( OP(A("order_rank"), "<", V(k)) )
        .project(A("order_rank"))
        .apply((order_A.e, C_INT64(A("order_rank"))))
    }

    // ------------------------------------------------
    // Build ------------------------------------------

    val ORDER = TOPPED
      .apply(
        (GDS.name_A.e, V(destination)),
        (GDS.value_A.e, C_STRING(order_A.e))
      )
      .apply(
        (A(GDS.nid1_D.name), LIB(gmql4scidb.hash, GDS.name_A.e)),
        (A(GDS.nid2_D.name), LIB(gmql4scidb.hash, OP(GDS.name_A.e,"+",C_STRING(V("0"))))),
        (A(GDS.vid1_D.name), LIB(gmql4scidb.hash, GDS.value_A.e)),
        (A(GDS.vid2_D.name), LIB(gmql4scidb.hash, OP(GDS.value_A.e,"+",C_STRING(V("0")))))
      )
      .redimension(GDS.meta_dimensions_D, GDS.meta_attributes_A)

    // ------------------------------------------------
    // Union ------------------------------------------

    val FILTER = TOPPED
      .redimension(List(GDS.sid_D), List(order_A))

    val RESULT = SOURCE
      .cross_join(FILTER, "SOURCE", "FILTER")((GDS.sid_D.alias("SOURCE").e, GDS.sid_D.alias("FILTER").e))
      .redimension(GDS.meta_dimensions_D, GDS.meta_attributes_A)
      .merge(ORDER)

    RESULT
  }

}
