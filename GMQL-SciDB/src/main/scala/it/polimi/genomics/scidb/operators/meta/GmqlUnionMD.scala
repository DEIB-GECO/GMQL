package it.polimi.genomics.scidb.operators.meta

import it.polimi.genomics.scidb.libraries.gmql4scidb
import it.polimi.genomics.scidb.operators.{GDS, GmqlMetaOperator}
import it.polimi.genomics.scidbapi.SciArray
import it.polimi.genomics.scidbapi.expression._
import it.polimi.genomics.scidbapi.script.SciScript

class GmqlUnionMD(left : GmqlMetaOperator,
                  right : GmqlMetaOperator,
                  leftAlias : String,
                  rightAlias : String)
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
    val LEFT = left.compute(script)
    val RIGHT = right.compute(script)

    // ------------------------------------------------
    // ------------------------------------------------

    def disambiguate(array:SciArray, name:String, side:String) : SciArray =
    {
      array
        .apply((GDS.name_A.label("new").e, OP(V(name+"."), "+", GDS.name_A.e)))
        .apply(
          (GDS.nid1_D.toAttribute().label("new").e, LIB(gmql4scidb.hash, GDS.name_A.label("new").e)),
          (GDS.nid2_D.toAttribute().label("new").e, LIB(gmql4scidb.hash, OP(GDS.name_A.label("new").e,"+",C_STRING(V("0")))))
        )
        .apply((GDS.sid_D.toAttribute().label("new").e, LIB(gmql4scidb.hash, OP(V(side), "+", C_STRING(GDS.sid_D.e)))))
        .redimension(
          List(
            GDS.nid1_D.label("new"),
            GDS.nid2_D.label("new"),
            GDS.vid1_D,
            GDS.vid2_D,
            GDS.sid_D.label("new")
          ), List(
            GDS.name_A.label("new"),
            GDS.value_A
          ))
        .cast(GDS.meta_dimensions_D, GDS.meta_attributes_A)
    }

    // ------------------------------------------------
    // union ------------------------------------------

    val LEFT_DIS = disambiguate(LEFT, leftAlias, "left")
    val RIGHT_DIS = disambiguate(RIGHT, rightAlias, "right")

    val RESULT = LEFT_DIS.merge(RIGHT_DIS)

    // ------------------------------------------------

    RESULT
  }

}
