package it.polimi.genomics.scidb.evaluators

import it.polimi.genomics.core.DataStructures.MetadataCondition.META_OP._
import it.polimi.genomics.core.DataStructures.MetadataCondition.MetadataCondition
import it.polimi.genomics.scidb.exception.UnsupportedOperationGmqlSciException
import it.polimi.genomics.scidb.libraries.gmql4scidb
import it.polimi.genomics.scidb.operators.GDS
import it.polimi.genomics.scidbapi.SciArray
import it.polimi.genomics.scidbapi.expression._
import it.polimi.genomics.scidbapi.schema.Attribute
import it.polimi.genomics.scidbapi.schema.DataType._

/**
  * This evaluator build a filter for meta condition evaluation
  */
object MetaConditionsEvaluator
{

  /**
    * Evaluator application
    *
    * @param source source array to be filtered
    * @param condition condition tree to be applied
    * @return
    */
  def apply(source:SciArray, condition:MetadataCondition) : SciArray =
  {
    val FILTER = condition match
    {
      // ------------------------------------------------
      // existential ------------------------------------

      case it.polimi.genomics.core.DataStructures.MetadataCondition.ContainAttribute(name) =>
        source
          .filter(AND(OP(GDS.nid1_D.e, "=", LIB(gmql4scidb.hash, V(name))), OP(GDS.nid2_D.e, "=", LIB(gmql4scidb.hash, OP(V(name),"+",C_STRING(V("0")))))))
          .redimension(List(GDS.sid_D), GDS.meta_attributes_A)

      case it.polimi.genomics.core.DataStructures.MetadataCondition.MissingAttribute(name) =>
        this(source,
          it.polimi.genomics.core.DataStructures.MetadataCondition.NOT(
            it.polimi.genomics.core.DataStructures.MetadataCondition.ContainAttribute(name)))

      // ------------------------------------------------
      // predicates -------------------------------------

      case it.polimi.genomics.core.DataStructures.MetadataCondition.Predicate(name, EQ, value) =>
        source
          .filter(
            AND(AND(OP(GDS.nid1_D.e, "=", LIB(gmql4scidb.hash, V(name))), OP(GDS.nid2_D.e, "=", LIB(gmql4scidb.hash, OP(V(name),"+",C_STRING(V("0")))))),
              AND(OP(GDS.vid1_D.e, "=", LIB(gmql4scidb.hash, V(value))), OP(GDS.vid2_D.e, "=", LIB(gmql4scidb.hash, OP(V(value),"+",C_STRING(V("0"))))))))
          .redimension(List(GDS.sid_D), GDS.meta_attributes_A)

      case it.polimi.genomics.core.DataStructures.MetadataCondition.Predicate(name, NOTEQ, value) =>
        this(source,
          it.polimi.genomics.core.DataStructures.MetadataCondition.AND(
            it.polimi.genomics.core.DataStructures.MetadataCondition.ContainAttribute(name),
            it.polimi.genomics.core.DataStructures.MetadataCondition.NOT(
              it.polimi.genomics.core.DataStructures.MetadataCondition.Predicate(name, EQ, value))))

      case it.polimi.genomics.core.DataStructures.MetadataCondition.Predicate(name, op, value) =>
        throw new UnsupportedOperationGmqlSciException("Predicates with operator '"+ op +"' are still not supported by the current implementation version")

      // ------------------------------------------------
      // operators --------------------------------------

      case it.polimi.genomics.core.DataStructures.MetadataCondition.AND(first, second) =>
        this(source, first).cross_join(this(source, second), "F", "S")((GDS.sid_D.alias("F").e, GDS.sid_D.alias("S").e))

      case it.polimi.genomics.core.DataStructures.MetadataCondition.OR(first, second) =>
        this(source, first).merge(this(source, second))

      case it.polimi.genomics.core.DataStructures.MetadataCondition.NOT(inner) =>
        source.redimension(List(GDS.sid_D), GDS.meta_attributes_A)
          .apply((A("sid_SEL"), GDS.sid_D.e)).project(A("sid_SEL"))
          .index_lookup(
            this(source, inner).apply((A("sid_SEL"), GDS.sid_D.e)).project(A("sid_SEL"))
              .redimension(List(GDS.sid_D),List(Attribute("sid_SEL", INT64, false))),
            A("sid_SEL"), A("exists"))
          .filter( FUN("is_null", A("exists")) )
    }

    FILTER.apply((GDS.null2, V(1))).project(GDS.null2).apply((GDS.null1, V(1))).project(GDS.null1)
  }

}
