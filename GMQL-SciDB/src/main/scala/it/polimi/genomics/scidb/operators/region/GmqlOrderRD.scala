package it.polimi.genomics.scidb.operators.region

import it.polimi.genomics.core.DataStructures.GroupMDParameters.Direction._
import it.polimi.genomics.core.DataStructures.GroupMDParameters.{TopG, Top, NoTop, TopParameter}
import it.polimi.genomics.scidb.libraries.gmql4scidb
import it.polimi.genomics.scidb.operators.{GDS, GmqlRegionOperator}
import it.polimi.genomics.scidbapi.SciArray
import it.polimi.genomics.scidbapi.SortingKeyword._
import it.polimi.genomics.scidbapi.expression._
import it.polimi.genomics.scidbapi.schema.{Dimension, Attribute}
import it.polimi.genomics.scidbapi.schema.DataType._
import it.polimi.genomics.scidbapi.script.SciScript


/**
  * This it.polimi.genomics.scidb.test.operator provides the implementation for
  * the region sorting operation
  *
  * @param source source dataset to sort
  * @param ordering ordering list
  * @param top top selection parameter
  */
class GmqlOrderRD(source : GmqlRegionOperator,
                  ordering : List[(Int,Direction)],
                  top : TopParameter)
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

    val rank_A = Attribute("rank", DOUBLE)
    val sort_D = Dimension("n", 0, None, Dimension.DEFAULT_CHUNK_LENGTH, Dimension.DEFAULT_OVERLAP)
    val sort_A = Attribute("sort", INT64, false)
    val order_A = Attribute("order", INT64, false)
    val group_D = Dimension("group", 0, None, 100, 0)

    // ------------------------------------------------
    // Sorting ----------------------------------------

    val sortings : List[(A, SortingKeyword)] =
      ordering.map(item => (
        SOURCE.getAttributes()(item._1).e,
        item._2 match {
          case ASC => asc
          case DESC => desc
        }
      ))

    val SORTED = SOURCE
      .unpack(D("synth"))
      .sort(sortings:_*)()
      .apply((sort_A.e, sort_D.e))
      .redimension(GDS.regions_dimensions_D, SOURCE.getAttributes() ::: List(sort_A))

    // ------------------------------------------------
    // Grouping ---------------------------------------

    val GROUPED = top match
    {
      case (NoTop()|Top(_)) => SORTED
          .rank(sort_A.e)(GDS.sid_D.e)
          .apply((order_A.e, C_INT64(A("sort_rank"))))
          .project(order_A.e)

      case TopG(k) => SORTED
        .apply(( group_D.toAttribute().e, LIB(gmql4scidb.hash, sortings.map(a => C_STRING(a._1):Expr).reduce((a1, a2) => OP(a1, "+", a2)))))
        .redimension(GDS.regions_dimensions_D ::: List(group_D), SORTED.getAttributes())
        .rank(sort_A.e)(GDS.sid_D.e, group_D.e)
        .apply((order_A.e, C_INT64(A("sort_rank"))))
        .project(order_A.e)
    }

    // ------------------------------------------------
    // Topping ----------------------------------------

    val TOPPED = top match
    {
      case NoTop() => GROUPED

      case Top(k) => GROUPED
        .filter( OP(order_A.e, "<=", V(k)) )

      case TopG(k) => GROUPED
        .filter( OP(order_A.e, "<=", V(k)) )
    }

    // ------------------------------------------------

    val RESULT = SOURCE
      .cross_join(TOPPED, "SOURCE", "ORDERS")(
        (GDS.sid_D.alias("SOURCE").e, GDS.sid_D.alias("ORDERS").e),
        (GDS.chr_D.alias("SOURCE").e, GDS.chr_D.alias("ORDERS").e),
        (GDS.left_D.alias("SOURCE").e, GDS.left_D.alias("ORDERS").e),
        (GDS.right_D.alias("SOURCE").e, GDS.right_D.alias("ORDERS").e),
        (GDS.strand_D.alias("SOURCE").e, GDS.strand_D.alias("ORDERS").e),
        (GDS.enumeration_D.alias("SOURCE").e, GDS.enumeration_D.alias("ORDERS").e)
      )

    RESULT
  }

}
