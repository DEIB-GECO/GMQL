package it.polimi.genomics.scidbapi.expression

import it.polimi.genomics.scidbapi.exception.{PropertyAmbiguitySciException, IllegalOperationSciException, UnsupportedOperationSciException, ResourceNotFoundSciException}
import it.polimi.genomics.scidbapi.expression.Null._
import it.polimi.genomics.scidbapi.schema.DataType._
import it.polimi.genomics.scidbapi.schema.{Attribute, Dimension}

/**
  * General expression
  */
abstract class Expr
{
  def eval(context:(List[Dimension],List[Attribute])) : (String, DataType)
}

// ------------------------------------------------------------

/**
  * Leaf expression based on a single attribute
  *
  * @param attribute
  */
case class A(attribute:String) extends Expr
{
  def eval(context:(List[Dimension],List[Attribute])) : (String, DataType) =
    ((attr:Attribute) => (attr.name, attr.datatype)).apply(extract(context))

  /**
    * Extract a attribute from the context given a name
    *
    * @param context extraction context
    * @return extracted attribute
    */
  def extract(context:(List[Dimension],List[Attribute])) : Attribute =
  {
    try{
      context._2.filter( (a:Attribute) => a.name == attribute ).head
    }catch{
      case ex:NoSuchElementException => throw new ResourceNotFoundSciException("Attribute '" + attribute +"' not found in the context")
    }
  }

  /**
    * Check if the attribute doesn't already exist in the context
 *
    * @param context extraction context
    * @return true if the attribute doesn't exist
    */
  def isNotInContext(context:(List[Dimension],List[Attribute])) : Boolean =
  {
    try{
      extract(context)
      throw new PropertyAmbiguitySciException("The attribute '"+ attribute +"' required already exist")
    }catch{
      case ex:ResourceNotFoundSciException => true
    }
  }

  /**
    * Extract the attribute name
    *
    * @return attribute name
    */
  def value() = attribute
}

/**
  * Leaf expression based on a single dimension
  *
  * @param dimension
  */
case class D(dimension:String) extends Expr
{
  def eval(context:(List[Dimension],List[Attribute])) : (String, DataType) = (extract(context).name, INT64)

  /**
    * Extract a dimension from the context given a name
    *
    * @param context extraction context
    * @return extracted dimension
    */
  def extract(context:(List[Dimension],List[Attribute])) : Dimension =
  {
    try{
      context._1.filter( (d:Dimension) => d.name == dimension ).head
    }catch{
      case ex:NoSuchElementException => throw new ResourceNotFoundSciException("Dimension '" + dimension +"' not found in the context")
    }
  }

  /**
    * Extract the dimension name
    *
    * @return dimension name
    */
  def value() = dimension
}

// ------------------------------------------------------------

/**
  * Leaf expression based on a single constant value
  *
  * @param value
  */
case class V(value:Any) extends Expr
{
  def eval(context:(List[Dimension],List[Attribute])) : (String, DataType) = value match
  {
    case v:Int => (v.toString, INT64)
    case v:Long => (v.toString, INT64)
    case v:Double => (v.toString, DOUBLE)
    case v:Boolean => (v.toString, BOOL)
    case v:String => ("'"+v+"'", STRING)

    case NULL_INT64 => ("null", INT64)
    case NULL_DOUBLE => ("null", DOUBLE)
    case NULL_BOOL => ("null", BOOL)
    case NULL_STRING => ("null", STRING)

    case NULL_BOUND => ("null", INT64)

    case v => throw new UnsupportedOperationSciException("Data type not supported, required value with type '"+(v.getClass.toString)+"', supported types are: int64, double, bool, string")
  }
}

// ------------------------------------------------------------

/**
  * Evaluates a binary it.polimi.genomics.scidb.test.operator
  *
  * @param e1
  * @param operator
  * @param e2
  */
case class OP(e1: Expr, operator:String, e2:Expr) extends Expr
{
  def eval(context:(List[Dimension],List[Attribute])) : (String, DataType) =
  {
    val (s1,t1) = e1.eval(context)
    val (s2,t2) = e2.eval(context)

    val returntype : DataType = (t1, t2, operator) match
    {
      // %, *, +, -, /
      case (INT64, INT64, ("%"|"*"|"+"|"-"|"/")) => INT64
      case ((DOUBLE|INT64), (DOUBLE|INT64), ("%"|"*"|"+"|"-"|"/")) => DOUBLE
      case (STRING, STRING, "+") => STRING

      // <, <=, <>, =, >, >=
      case ((DOUBLE | INT64), (DOUBLE | INT64), ("<"|"<="|"<>"|"="|">"|">=")) => BOOL
      case (STRING, STRING, ("<"|"<="|"<>"|"="|">"|">=")) => BOOL
      case (BOOL, BOOL, ("<"|"<="|"<>"|"="|">"|">=")) => BOOL

      // and, or
      case (BOOL, BOOL, ("and"|"or")) => BOOL

      // default
      case _ => throw new UnsupportedOperationSciException("Operator not supported, required '"+operator+"'("+t1+","+t2+")'")
    }

    ( "("+ s1 + " " + operator + " " + s2 + ")" , returntype )
  }
}

/**
  * Evaluates an unary it.polimi.genomics.scidb.test.operator
  *
  * @param operator
  * @param e1
  */
case class UOP(operator:String, e1:Expr) extends Expr
{
  def eval(context: (List[Dimension], List[Attribute])): (String, DataType) =
  {
    val (s1,t1) = e1.eval(context)

    val returntype : DataType = (t1, operator) match
    {
      // -
      case (INT64, "-") => INT64
      case (DOUBLE, "-") => DOUBLE

      // default
      case _ => throw new UnsupportedOperationSciException("Operator not supported, required '"+operator+"'("+t1+")'")
    }

    ( "("+ operator + s1 + ")" , returntype )
  }
}


// ------------------------------------------------------------

/**
  * Evaluates a generic function given a sequence of arguments
  *
  * @param function function name
  * @param args arguments
  */
case class FUN(function:String, args:Expr*) extends Expr
{
  def eval(context:(List[Dimension],List[Attribute])) : (String, DataType) =
  {
    // arguments evaluation -----------------------
    val args_evaluated = args.map((e:Expr) => e.eval(context))

    // type check ---------------------------------
    val returnType : DataType = (function, args_evaluated.map((item:(String,DataType)) => item._2).toList) match
    {
      // abs, ceil, floor
      case ("abs", INT64 :: Nil) => INT64
      case ("abs", DOUBLE :: Nil) => DOUBLE
      case ("ceil", (INT64|DOUBLE) :: Nil) => INT64
      case ("floor", (INT64|DOUBLE) :: Nil) => INT64

      // trigonometrics
      case ("acos", (INT64|DOUBLE) :: Nil) => DOUBLE
      case ("asin", (INT64|DOUBLE) :: Nil) => DOUBLE
      case ("atan", (INT64|DOUBLE) :: Nil) => DOUBLE

      case ("cos", (INT64|DOUBLE) :: Nil) => DOUBLE
      case ("sin", (INT64|DOUBLE) :: Nil) => DOUBLE
      case ("tan", (INT64|DOUBLE) :: Nil) => DOUBLE

      // exp
      case ("exp", (INT64|DOUBLE) :: Nil) => DOUBLE

      // string
      case ("first_index", STRING :: Nil) => INT64
      case ("first_index", STRING :: STRING :: Nil) => INT64
      case ("last_index", STRING :: Nil) => INT64
      case ("last_index", STRING :: STRING :: Nil) => INT64

      case ("high", STRING :: Nil) => INT64
      case ("high", STRING :: STRING :: Nil) => INT64
      case ("low", STRING :: Nil) => INT64
      case ("low", STRING :: STRING :: Nil) => INT64

      case ("length", STRING :: Nil) => INT64
      case ("length", STRING :: STRING :: Nil) => INT64

      // verification
      case ("is_null", _ :: Nil) => BOOL
      case ("not", BOOL :: Nil) => BOOL

      // system
      case ("instanceid", Nil) => INT64

      // default
      case (fun,args) => throw new UnsupportedOperationSciException("Function not supported, required '" + fun + "(" + args.mkString(",") + ")'")
    }

    // result construction ------------------------
    val returnString = function + "(" + args_evaluated.map((item:(String,DataType)) => item._1).toList.mkString(",") + ")"
    (returnString,returnType)
  }
}


// ------------------------------------------------------------


/**
  * Evaluates a generinc function coming from an external library
  *
  * @param function function
  * @param args arguments
  */
case class LIB(function:SciFunction, args:Expr*) extends Expr
{
  def eval(context:(List[Dimension],List[Attribute])) : (String, DataType) =
  {
    // arguments evaluation -----------------------
    val args_evaluated = args.map((e:Expr) => e.eval(context))

    // type check ---------------------------------
    val returnType =
      if(function.input == args_evaluated.map((item:(String,DataType)) => item._2).toList)
        function.output
      else
        throw new UnsupportedOperationSciException("Function not supported, required '" + function.name + "(" + function.input.mkString(",") + "):"+ function.output +"'")

    // result construction ------------------------
    val returnString = function.name + "(" + args_evaluated.map((item:(String,DataType)) => item._1).toList.mkString(",") + ")"
    (returnString,returnType)
  }
}


// ------------------------------------------------------------


/**
  * Evaluates an if construct
  *
  * @param condition condition
  * @param _then value if condition is true
  * @param _else value if condition is false
  */
case class IF(condition:Expr, _then:Expr, _else:Expr) extends Expr
{
  def eval(context:(List[Dimension],List[Attribute])) : (String, DataType) =
  {
    // arguments evaluation -----------------------
    val ( conditionS, conditionT ) = condition.eval(context)
    val ( thenS, thenT ) = _then.eval(context)
    val ( elseS, elseT ) = _else.eval(context)

    // type check ---------------------------------
    val returnType : DataType = (conditionT,thenT,elseT) match
    {
      case (BOOL, INT64, INT64) => INT64
      case (BOOL, (INT64|DOUBLE), (INT64|DOUBLE)) => DOUBLE
      case (BOOL, BOOL, BOOL) => BOOL
      case (BOOL, STRING, STRING) => STRING

      // default
      case (BOOL, t1, t2) => throw new UnsupportedOperationSciException("Arguments not supported, required 'if(bool) then "+ t1 +" else "+ t2 +"'")
      case (datatype, _, _) => throw new IllegalOperationSciException("If statement should have boolean condition, actual data type '"+ datatype +"'")
    }

    // result construction ------------------------
    val returnS = "iif(" + conditionS + "," + thenS + "," + elseS + ")"
    (returnS, returnType)
  }
}


// ------------------------------------------------------------


/**
  * Evaluates the explicit casting operation on expressions
  *
  * @param expression expression to be casted
  * @param castedType casting type
  */
case class CAST(expression:Expr, castedType:DataType) extends Expr
{
  def eval(context:(List[Dimension],List[Attribute])) : (String, DataType) =
  {
    // arguments evaluation -----------------------
    val ( expressionS, expressionT ) = expression.eval(context)

    // type check ---------------------------------
    val returnType : DataType = (expressionT, castedType) match
    {
      case (from, to) if from == to => castedType

      case (INT64, (BOOL|DOUBLE|STRING)) => castedType
      case (DOUBLE, (INT64|STRING)) => castedType
      case (STRING, (INT64|DOUBLE)) => castedType

      case (UINT64, INT64) => castedType

      case (from, to) => throw new UnsupportedOperationSciException("Casting operation not supported, it's not possible caset '"+ from +"' into '"+ to +"'")
    }

    // result construction ------------------------
    val returnS = castedType + "(" + expressionS + ")"
    (returnS, returnType)
  }
}