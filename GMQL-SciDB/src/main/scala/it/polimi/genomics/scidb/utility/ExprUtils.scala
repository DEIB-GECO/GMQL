package it.polimi.genomics.scidb.utility

import it.polimi.genomics.scidbapi.expression.{OP, IF, V, Expr}


/**
  * This object provides some methods to define
  * MACROS for expressions
  */
object ExprUtils
{

  /**
    * Return the expresion to check the intersection
    * between two regions
    *
    * @param l1 left end of region 1
    * @param r1 right end of region 1
    * @param l2 left end of region 2
    * @param r2 right end of region 2
    * @return
    */
  def intersection(l1:Expr, r1:Expr, l2:Expr, r2:Expr) : Expr = OP(l2, ">", r1)
    //IF( OP(l2, ">", l1), OP(r1, ">", l2), OP(r2, ">", l1) )

}
