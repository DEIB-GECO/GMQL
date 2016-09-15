package it.polimi.genomics.scidb.operators.region

import it.polimi.genomics.scidb.libraries.gmql4scidb
import it.polimi.genomics.scidb.operators.{GDS, GmqlRegionOperator}
import it.polimi.genomics.scidb.utility.DebugUtils
import it.polimi.genomics.scidbapi.SciArray
import it.polimi.genomics.scidbapi.expression._
import it.polimi.genomics.scidbapi.script.SciScript


/**
  * This it.polimi.genomics.scidb.test.operator implement the region union gmql function
  *
  * @param left left source dataset
  * @param right right source dataset
  * @param format insertion format
  */
class GmqlUnionRD(left : GmqlRegionOperator,
                  right : GmqlRegionOperator,
                  format : List[Int])
  extends GmqlRegionOperator {
  /**
    * Apply the it.polimi.genomics.scidb.test.operator producing the result, it requires the
    * child nodes results if necessary
    *
    * @param script Context script
    */
  override def apply(script: SciScript): SciArray =
  {
    val LEFT = left.compute(script)
    val RIGHT = right.compute(script)

    // ------------------------------------------------
    // ------------------------------------------------

    def disambiguate(array:SciArray, side:String) : SciArray =
    {
      array
        .apply((GDS.sid_D.toAttribute().label("new").e, LIB(gmql4scidb.hash, OP(V(side), "+", C_STRING(GDS.sid_D.e)))))
        .redimension(
          List(
            GDS.sid_D.label("new"),
            GDS.chr_D, GDS.left_D, GDS.right_D, GDS.strand_D, GDS.enumeration_D
          ), array.getAttributes())
        .cast(GDS.regions_dimensions_D, LEFT.getAttributes().map(_.label(side)))
    }

    val LEFT_prepared = disambiguate(LEFT, "left")
    val RIGHT_prepared = disambiguate(RIGHT, "right")

    // ------------------------------------------------
    // new schema -------------------------------------

    val leftApplication = format.zipWithIndex.map(item => item._1 match {
      case index if(index < LEFT_prepared.getAttributes().size) => null
      case index => (RIGHT_prepared.getAttributes()(item._2).e, V(Null.nil(RIGHT_prepared.getAttributes()(item._2).datatype)))
    }).filter(_ != null)

    val rightApplication = (format.zipWithIndex.map(item => item._1 match {
      case index if(index >= LEFT_prepared.getAttributes().size) => null
      case index if(index < 0) => null
      case index => (LEFT_prepared.getAttributes()(item._2).e, RIGHT_prepared.getAttributes()(item._1).e)
    }) ++ (0 to LEFT_prepared.getAttributes().size-1).diff(format).map(item =>
      (LEFT_prepared.getAttributes()(item).e, V(Null.nil(LEFT_prepared.getAttributes()(item).datatype)))
    )).filter(_ != null)

    // ------------------------------------------------
    // application ------------------------------------

    val LEFT_applied = if(leftApplication.isEmpty) LEFT_prepared else LEFT_prepared.apply(leftApplication:_*)
    val RIGHT_applied = if(rightApplication.isEmpty) RIGHT_prepared else RIGHT_prepared.apply(rightApplication:_*)

    val RESULT = RIGHT_applied
      .project(LEFT_applied.getAttributes().map(_.e):_*)
      .cast(GDS.regions_dimensions_D, LEFT_applied.getAttributes())
      .merge(LEFT_applied
        .cast(GDS.regions_dimensions_D, LEFT_applied.getAttributes()))

    // ------------------------------------------------

    RESULT
  }

}
