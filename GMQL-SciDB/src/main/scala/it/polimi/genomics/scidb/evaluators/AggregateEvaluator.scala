package it.polimi.genomics.scidb.evaluators

import it.polimi.genomics.scidbapi.aggregate.{Aggregate, AGGR_COUNT, AGGR}
import it.polimi.genomics.scidbapi.expression.{C_INT64, Expr, A}

/**
  * Created by Cattani Simone on 15/04/16.
  * Email: simone.cattani@mail.polimi.it
  *
  */
object AggregateEvaluator
{
  def apply(fun:String, in:A, out:A) : (List[Aggregate], Option[(A,Expr)]) = fun match
  {
    case "COUNT"|"BAG" => (List(AGGR_COUNT(in, A(out.attribute+"_tmp"))), Some((out, C_INT64(A(out.attribute+"_tmp")))))

    case "MIN"|"MAX"|"AVG"|"SUM" => (List(AGGR(fun.toLowerCase, in, out)), None)

    case _ => throw new UnsupportedOperationException("Aggregate "+fun+" not supported")
  }
}
