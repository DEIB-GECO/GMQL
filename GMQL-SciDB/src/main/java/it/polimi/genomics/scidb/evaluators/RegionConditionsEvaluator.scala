package it.polimi.genomics.scidb.evaluators

import it.polimi.genomics.core.DataStructures.RegionCondition.{MetaAccessor, RegionCondition}
import it.polimi.genomics.scidb.operators.GDS
import it.polimi.genomics.scidbapi.SciArray
import it.polimi.genomics.scidbapi.expression._
import it.polimi.genomics.scidbapi.schema.DataType.DataType
import it.polimi.genomics.scidbapi.schema.DataType._

/**
  * This evaluator build a filter for region condition evaluation
  */
object RegionConditionsEvaluator
{
  /**
    * Evaluator application
    *
    * @param source scidb source array
    * @param condition condition tree to be applied
    * @return
    */
  def apply(source:SciArray, condition:RegionCondition) : (Expr, Set[(String,DataType)]) = condition match
  {
    // ------------------------------------------------
    // coordinates ------------------------------------

    case it.polimi.genomics.core.DataStructures.RegionCondition.ChrCondition(name) => ( OP(GDS.CHR_A.e, "=", V(name)), Set() )
    case it.polimi.genomics.core.DataStructures.RegionCondition.LeftEndCondition(op, v) => ( OP(GDS.left_D.e, operator(op), value(v, INT64)._1), value(v, INT64)._2 )
    case it.polimi.genomics.core.DataStructures.RegionCondition.RightEndCondition(op, v) => ( OP(GDS.right_D.e, operator(op), value(v, INT64)._1), value(v, INT64)._2 )
    case it.polimi.genomics.core.DataStructures.RegionCondition.StrandCondition(v) => ( OP(GDS.strand_D.e, "=", V(strand(v))), Set() )

    case it.polimi.genomics.core.DataStructures.RegionCondition.StartCondition(op, v) => ( IF( OP(GDS.strand_D.e, "<>", V(0)), OP(GDS.left_D.e, operator(op), value(v, INT64)._1), OP(GDS.right_D.e, operator(op), value(v, INT64)._1) ), value(v, INT64)._2 )
    case it.polimi.genomics.core.DataStructures.RegionCondition.StopCondition(op, v) => ( IF( OP(GDS.strand_D.e, "<>", V(0)), OP(GDS.right_D.e, operator(op), value(v, INT64)._1), OP(GDS.left_D.e, operator(op), value(v, INT64)._1) ), value(v, INT64)._2 )

    // ------------------------------------------------
    // predicates -------------------------------------

    case it.polimi.genomics.core.DataStructures.RegionCondition.Predicate(position, op, v) => ( OP(source.getAttributes()(position).e, operator(op), value(v, source.getAttributes()(position).datatype)._1), value(v, source.getAttributes()(position).datatype)._2 )

    // ------------------------------------------------
    // operators --------------------------------------

    case it.polimi.genomics.core.DataStructures.RegionCondition.AND(left, right) => {
      val leftNode = this(source, left)
      val rightNode = this(source, right)
      ( AND(leftNode._1, rightNode._1), leftNode._2 ++ rightNode._2 )
    }
    case it.polimi.genomics.core.DataStructures.RegionCondition.OR(left, right) => {
      val leftNode = this(source, left)
      val rightNode = this(source, right)
      ( OR(leftNode._1, rightNode._1), leftNode._2 ++ rightNode._2 )
    }
    case it.polimi.genomics.core.DataStructures.RegionCondition.NOT(condition) => {
      val conditionNode = this(source, condition)
      ( NOT(conditionNode._1), conditionNode._2 )
    }

    // ------------------------------------------------
    // ------------------------------------------------

    case condition => throw new UnsupportedOperationException("Operation '"+ condition +"' is not currently supported by the implementation")
  }

  /**
    * Returns the scidb code for the required operator
    *
    * @param op REG_OP operation
    * @return scidb string code
    */
  def operator(op : it.polimi.genomics.core.DataStructures.RegionCondition.REG_OP.REG_OP) : String = op match
  {
    case it.polimi.genomics.core.DataStructures.RegionCondition.REG_OP.EQ     => "="
    case it.polimi.genomics.core.DataStructures.RegionCondition.REG_OP.NOTEQ  => "<>"
    case it.polimi.genomics.core.DataStructures.RegionCondition.REG_OP.GT     => ">"
    case it.polimi.genomics.core.DataStructures.RegionCondition.REG_OP.GTE    => ">="
    case it.polimi.genomics.core.DataStructures.RegionCondition.REG_OP.LT     => "<"
    case it.polimi.genomics.core.DataStructures.RegionCondition.REG_OP.LTE    => "<="
  }

  /**
    * Returns the scidb code for a scidb strand symbol
    *
    * @param symbol strand symbol
    * @return code
    */
  def strand(symbol : String) : Int = symbol match
  {
    case "+" => 2
    case "*" => 1
    case "-" => 0
  }

  /**
    * Returns the leaf value for a condition
    *
    * @param v value
    * @param dataType value data type
    * @return
    */
  def value(v : Any, dataType : DataType) : (Expr, Set[(String,DataType)]) = v match
  {
    case MetaAccessor(name) => dataType match
    {
      case INT64 => (C_INT64(A(name+"_"+dataType)), Set((name, dataType)))
      case DOUBLE => (C_DOUBLE(A(name+"_"+dataType)), Set((name, dataType)))
      case BOOL => (C_BOOL(A(name+"_"+dataType)), Set((name, dataType)))
      case STRING => (C_STRING(A(name+"_"+dataType)), Set((name, dataType)))
    }
    case v => (V(v), Set())
  }

}
