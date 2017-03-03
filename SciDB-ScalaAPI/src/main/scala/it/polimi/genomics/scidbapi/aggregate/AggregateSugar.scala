package it.polimi.genomics.scidbapi.aggregate

import it.polimi.genomics.scidbapi.expression.A
import it.polimi.genomics.scidbapi.schema.DataType._
import it.polimi.genomics.scidbapi.schema.{Attribute, Dimension}


/**
  * Provides syntactic sugar for avg aggregator
  *
  * @param attribute aggregation attribute
  * @param name new attribute name
  */
case class AGGR_AVG(attribute:A, name:A = null) extends Aggregate("avg", attribute, name) {
  override def eval(context:(List[Dimension],List[Attribute])) : (String, DataType, String) = super.eval(context)
}

/**
  * Provides syntactic sugar for count aggregator
  *
  * @param attribute aggregation attribute
  * @param name new attribute name
  */
case class AGGR_COUNT(attribute:A, name:A = null) extends Aggregate("count", attribute, name) {
  override def eval(context:(List[Dimension],List[Attribute])) : (String, DataType, String) = super.eval(context)
}

/**
  * Provides syntactic sugar for max aggregator
  *
  * @param attribute aggregation attribute
  * @param name new attribute name
  */
case class AGGR_MAX(attribute:A, name:A = null) extends Aggregate("max", attribute, name) {
  override def eval(context:(List[Dimension],List[Attribute])) : (String, DataType, String) = super.eval(context)
}

/**
  * Provides syntactic sugar for median aggregator
  *
  * @param attribute aggregation attribute
  * @param name new attribute name
  */
case class AGGR_MEDIAN(attribute:A, name:A = null) extends Aggregate("median", attribute, name) {
  override def eval(context:(List[Dimension], List[Attribute])): (String, DataType, String) = super.eval(context)
}

/**
  * Provides syntactic sugar for min aggregator
  *
  * @param attribute aggregation attribute
  * @param name new attribute name
  */
case class AGGR_MIN(attribute:A, name:A = null) extends Aggregate("min", attribute, name) {
  override def eval(context:(List[Dimension],List[Attribute])) : (String, DataType, String) = super.eval(context)
}

/**
  * Provides syntactic sugar for prod aggregator
  *
  * @param attribute aggregation attribute
  * @param name new attribute name
  */
case class AGGR_PROD(attribute:A, name:A = null) extends Aggregate("prod", attribute, name) {
  override def eval(context:(List[Dimension],List[Attribute])) : (String, DataType, String) = super.eval(context)
}

/**
  * Provides syntactic sugar for stdev aggregator
  *
  * @param attribute aggregation attribute
  * @param name new attribute name
  */
case class AGGR_STDEV(attribute:A, name:A = null) extends Aggregate("stdev", attribute, name) {
  override def eval(context:(List[Dimension],List[Attribute])) : (String, DataType, String) = super.eval(context)
}

/**
  * Provides syntactic sugar for sum aggregator
  *
  * @param attribute aggregation attribute
  * @param name new attribute name
  */
case class AGGR_SUM(attribute:A, name:A = null) extends Aggregate("sum", attribute, name) {
  override def eval(context:(List[Dimension],List[Attribute])) : (String, DataType, String) = super.eval(context)
}

/**
  * Provides syntactic sugar for var aggregator
  *
  * @param attribute aggregation attribute
  * @param name new attribute name
  */
case class AGGR_VAR(attribute:A, name:A = null) extends Aggregate("var", attribute, name) {
  override def eval(context:(List[Dimension],List[Attribute])) : (String, DataType, String) = super.eval(context)
}