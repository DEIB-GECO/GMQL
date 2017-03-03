package it.polimi.genomics.scidbapi.expression

import it.polimi.genomics.scidbapi.schema.DataType._
import it.polimi.genomics.scidbapi.schema.{DataType, Attribute, Dimension}


// ------------------------------------------------------------


/**
  * Evaluates if e1 AND e2 are true
  *
  * @param e1
  * @param e2
  */
case class AND(e1:Expr, e2:Expr) extends Expr {
  def eval(context:(List[Dimension],List[Attribute])) : (String, DataType) = OP(e1, "and", e2).eval(context)
}

/**
  * Evaluates if e1 OR e2 are true
  *
  * @param e1
  * @param e2
  */
case class OR(e1:Expr, e2:Expr) extends Expr {
  def eval(context:(List[Dimension],List[Attribute])) : (String, DataType) = OP(e1, "or", e2).eval(context)
}

/**
  * Evaluates if NOT e is true
  *
  * @param e
  */
case class NOT(e:Expr) extends Expr {
  def eval(context:(List[Dimension],List[Attribute])) : (String, DataType) = FUN("not", e).eval(context)
}


// ------------------------------------------------------------


/**
  * Syntactic sugar to cast into booleans
  *
  * @param expression expression to be casted
  */
case class C_BOOL(expression:Expr) extends Expr {
  def eval(context: (List[Dimension], List[Attribute])): (String, DataType) = CAST(expression, BOOL).eval(context)
}

/**
  * Syntactic sugar to cast into int64
  *
  * @param expression expression to be casted
  */
case class C_INT64(expression:Expr) extends Expr {
  def eval(context: (List[Dimension], List[Attribute])): (String, DataType) = CAST(expression, INT64).eval(context)
}

/**
  * Syntactic sugar to cast into double
  *
  * @param expression expression to be casted
  */
case class C_DOUBLE(expression:Expr) extends Expr {
  def eval(context: (List[Dimension], List[Attribute])): (String, DataType) = CAST(expression, DOUBLE).eval(context)
}

/**
  * Syntactic sugar to cast into strings
  *
  * @param expression expression to be casted
  */
case class C_STRING(expression:Expr) extends Expr {
  def eval(context: (List[Dimension], List[Attribute])): (String, DataType) = CAST(expression, STRING).eval(context)
}
