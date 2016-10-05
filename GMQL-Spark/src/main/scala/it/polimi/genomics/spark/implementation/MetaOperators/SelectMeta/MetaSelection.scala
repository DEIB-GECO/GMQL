package it.polimi.genomics.spark.implementation.MetaOperators.SelectMeta

import it.polimi.genomics.core.DataStructures.MetadataCondition._
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.exception.SelectFormatException
import org.apache.spark.rdd.RDD

/**
 * Created by Abdulrahman Kaitoua on 23/07/15.
 * Email: abdulrahman.kaitoua@polimi.it
 *
 */
trait MetaSelection {
  type ID = Long
  val ALL = "*"
  @throws[SelectFormatException]
  def applyMetaSelect(metaCondition: MetadataCondition, input: RDD[MetaType]): RDD[Long] = {
    metaCondition match {
      case metaCondition: Predicate => {
        val predicate = metaCondition.asInstanceOf[Predicate]
        predicate.operator match {
          case META_OP.EQ => applyMetaPredicateEQ(predicate, input)
          case META_OP.NOTEQ => applyMetaPredicateNOTEQ(predicate, input)
          case META_OP.GT =>  applyMetaPredicateGT(predicate, input)
          case META_OP.GTE => applyMetaPredicateGTE(predicate, input)
          case META_OP.LT =>  applyMetaPredicateLT(predicate, input)
          case META_OP.LTE =>  applyMetaPredicateLTE(predicate, input)
        }
      }

      case missing : MissingAttribute => {
        applyMetaSelect(NOT(ContainAttribute(missing.attribute)), input)
      }

      case contain : ContainAttribute => {
        applyContainAttribute(contain.attribute, input)
      }

      case metaCondition: NOT => {
        val subRes = applyMetaSelect(metaCondition.predicate, input)
        input
          .cogroup(subRes.map(x=>(x,0))).flatMap{x=> val left = x._2._1; val right = x._2._2
          if(right.size.equals(0)){
            Some(x._1)
          }else None
        }
      }

      case metaCondition: OR => applyMetaConditionOR(metaCondition.first_predicate, metaCondition.second_predicate, input)
      case metaCondition: AND => applyMetaConditionAND(metaCondition.first_predicate, metaCondition.second_predicate, input)
    }
  }
  @throws[SelectFormatException]
  def applyContainAttribute(name : String, input : RDD[MetaType]) : RDD[Long] = {
    input.filter(_._2._1.endsWith(name)).distinct().map(v => v._1)
  }

  //Predicate evaluation methods

  @throws[SelectFormatException]
  def applyMetaPredicateEQ(predicate: Predicate, input: RDD[MetaType]): RDD[Long] = {
   castDoubleOrString(predicate.value) match {
      case value: Double =>
        val inputFilteredAtt = input.filter(_._2._1.endsWith(predicate.attribute_name));
        println("\n\n\n\nhello"+value+"\n\n\n")
        if(value.equals(ALL)) inputFilteredAtt.keys.distinct()
        else inputFilteredAtt.filter{a  =>
        try{a._2._2.toDouble.equals(value)} catch {case _ : Throwable => false}
      }.keys.distinct()
      case value: String => val inputFilteredAtt = input.filter(_._2._1.toLowerCase.endsWith(predicate.attribute_name.toLowerCase));
        if(value.equals(ALL)) inputFilteredAtt.keys.distinct()
        else inputFilteredAtt.filter( _._2._2.toLowerCase().equals(value.toLowerCase())).keys
    }
  }

  @throws[SelectFormatException]
  def applyMetaPredicateNOTEQ(predicate: Predicate, input: RDD[MetaType]): RDD[Long] = {
    castDoubleOrString(predicate.value) match {
      case value: Double => input.filter(_._2._1.endsWith(predicate.attribute_name)).filter { a =>
        try {!a._2._2.toDouble.equals(value)} catch {case _: Throwable => false}}.keys
      case value: String => input.filter(_._2._1.toLowerCase.endsWith(predicate.attribute_name.toLowerCase)).filter(!_._2._2.toLowerCase.equals(value.toLowerCase)).keys
    }
  }
  //Predicate evaluation methods
  @throws[SelectFormatException]
  def applyMetaPredicateLT(predicate: Predicate, input: RDD[MetaType]): RDD[Long] = {
    castDoubleOrString(predicate.value) match {
      case value: Double =>
        input.filter(_._2._1.endsWith(predicate.attribute_name)).filter{a =>
          try{a._2._2.toDouble < value} catch {case _ : Throwable => false}}.keys
      case value: String => throw SelectFormatException.create("Your SELECT statement cannot be executed: yuo are doing a < comparison between string. Attribute = " + predicate.value)
    }
  }

  @throws[SelectFormatException]
  def applyMetaPredicateLTE(predicate: Predicate, input: RDD[MetaType]): RDD[Long] = {
    castDoubleOrString(predicate.value) match {
      case value: Double => input.filter(_._2._1.endsWith(predicate.attribute_name)).filter{a=>
        try{a._2._2.toDouble <= value} catch {case _ : Throwable => false}}.keys
      case value: String => throw SelectFormatException.create("Your SELECT statement cannot be executed: yuo are doing a <= comparison between string. Attribute = " + predicate.value)
    }
  }

  @throws[SelectFormatException]
  def applyMetaPredicateGT(predicate: Predicate, input: RDD[MetaType]): RDD[Long] = {
    castDoubleOrString(predicate.value) match {
      case value : Double =>
        input.filter(_._2._1.endsWith(predicate.attribute_name)).filter{a =>
          try{a._2._2.toDouble > value} catch {case _ : Throwable => false}}.keys
      case value : String => throw SelectFormatException.create("Your SELECT statement cannot be executed: yuo are doing a > comparison between string. Attribute = " + predicate.value)
    }
  }

  @throws[SelectFormatException]
  def applyMetaPredicateGTE(predicate: Predicate, input: RDD[MetaType]): RDD[Long] = {
    castDoubleOrString(predicate.value) match {
      case value : Double => input.filter(_._2._1.endsWith(predicate.attribute_name)).filter{a  =>
        try{a._2._2.toDouble >= value} catch {case _ : Throwable => false}}.keys
      case value : String => throw SelectFormatException.create("Your SELECT statement cannot be executed: yuo are doing a >= comparison between string. Attribute = " + predicate.value)
    }
  }

  //Other usefull methods

  def castDoubleOrString(value : Any) : Any = {
    try{
      value.toString.toDouble
    } catch {
      case e : Throwable => value.toString
    }
  }
  //Composed metacondition evaluation methods

  @throws[SelectFormatException]
  def applyMetaConditionOR(meta1: MetadataCondition, meta2: MetadataCondition, input: RDD[MetaType]): RDD[Long] = {
    applyMetaSelect(meta1, input).union(applyMetaSelect(meta2, input)).distinct()
  }

  @throws[SelectFormatException]
  def applyMetaConditionAND(meta1: MetadataCondition, meta2: MetadataCondition, input: RDD[MetaType]): RDD[Long] = {
    applyMetaSelect(meta1, input).map(x=>(x,0)).join(applyMetaSelect(meta2, input).map(x=>(x,0))).keys
  }
}
