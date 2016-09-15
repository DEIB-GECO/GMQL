package it.polimi.genomics.scidb.operators.meta

import it.polimi.genomics.core.DataStructures.MetadataCondition.MetadataCondition
import it.polimi.genomics.scidb.libraries.gmql4scidb
import it.polimi.genomics.scidb.operators.{GDS, GmqlMetaOperator}
import it.polimi.genomics.scidb.utility.DebugUtils
import it.polimi.genomics.scidbapi.SciArray
import it.polimi.genomics.scidbapi.expression._
import it.polimi.genomics.scidbapi.script.SciScript


class GmqlProjectMD(source:GmqlMetaOperator, attributes:List[String])
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

    val PROJECTION = if(attributes.isEmpty) SOURCE else SOURCE
      .filter(
        attributes.map(name =>
          AND(
            OP(GDS.nid1_D.e, "=", LIB(gmql4scidb.hash, V(name))),
            OP(GDS.nid2_D.e, "=", LIB(gmql4scidb.hash, OP(V(name),"+",C_STRING(V("0")))))
          )
        ).reduce((e1:Expr,e2:Expr) => OR(e1,e2))
      )


    // ------------------------------------------------
    // ------------------------------------------------

    PROJECTION
  }

}
