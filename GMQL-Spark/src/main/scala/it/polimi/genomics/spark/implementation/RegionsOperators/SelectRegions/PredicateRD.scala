package it.polimi.genomics.spark.implementation.RegionsOperators


import it.polimi.genomics.core.DataStructures.RegionCondition.REG_OP._
import it.polimi.genomics.core.DataStructures.RegionCondition._
import it.polimi.genomics.core.DataStructures.{MetaOperator, RegionCondition}
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.core.{GDouble, GNull, GString, GValue}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext


/**
  * Created by Abdulrahman Kaitoua on 02/06/15.
  * Email: abdulrahman.kaitoua@polimi.it
  *
  */
object PredicateRD {

  var executor: GMQLSparkExecutor = null

  def optimizeConditionTree(regionCondition: RegionCondition, not: Boolean, filteredMeta: Option[MetaOperator], sc: SparkContext): RegionCondition = {
    regionCondition match {

      case cond: RegionCondition.NOT => {
        NOT(optimizeConditionTree(cond.predicate, not, filteredMeta, sc))
      }

      case cond: RegionCondition.OR =>
        RegionCondition.OR(optimizeConditionTree(cond.first_predicate, not, filteredMeta, sc), optimizeConditionTree(cond.second_predicate, not, filteredMeta, sc))


      case cond: RegionCondition.AND =>
        RegionCondition.AND(optimizeConditionTree(cond.first_predicate, not, filteredMeta, sc), optimizeConditionTree(cond.second_predicate, not, filteredMeta, sc))

      case predicate: RegionCondition.Predicate => {

        val value = predicate.value match {

          case v: MetaAccessor => {
            val meta = executor.implement_md(filteredMeta.get, sc)
            meta.filter(_._2._1.equals(predicate.value.asInstanceOf[MetaAccessor].attribute_name)).distinct.collect
          }

          case v: Any => {
            predicate.value
          }
        }

        predicate.operator match {
          case REG_OP.EQ => RegionCondition.Predicate(predicate.position, RegionCondition.REG_OP.EQ, value)
          case REG_OP.NOTEQ => RegionCondition.Predicate(predicate.position, RegionCondition.REG_OP.NOTEQ, value)
          case REG_OP.GT => RegionCondition.Predicate(predicate.position, RegionCondition.REG_OP.GT, value)
          case REG_OP.GTE => RegionCondition.Predicate(predicate.position, RegionCondition.REG_OP.GTE, value)
          case REG_OP.LT => RegionCondition.Predicate(predicate.position, RegionCondition.REG_OP.LT, value)
          case REG_OP.LTE => RegionCondition.Predicate(predicate.position, RegionCondition.REG_OP.LTE, value)
        }
      }
      case noNeedForOptimization: RegionCondition => {
        noNeedForOptimization
      }
    }
  }

  @throws[SelectFormatException]
  def applyRegionSelect(regionCondition: RegionCondition, input: GRECORD): Boolean = {
    regionCondition match {
      case chrCond: ChrCondition => input._1._2.equals(chrCond.chr_name)
      case strandCond: StrandCondition => input._1._5.equals(strandCond.strand(0))
      case leftEndCond: LeftEndCondition => applyRegionPredicate(leftEndCond.op, leftEndCond.value, GDouble(input._1._3), input._1._1)
      case rightEndCond: RightEndCondition => applyRegionPredicate(rightEndCond.op, rightEndCond.value, GDouble(input._1._4), input._1._1)
      case startCond: StartCondition =>
        input._1._5 match {
          case '*' => applyRegionPredicate(startCond.op, startCond.value, GDouble(input._1._3), input._1._1)
          case '+' => applyRegionPredicate(startCond.op, startCond.value, GDouble(input._1._4), input._1._1)
          case '-' => applyRegionPredicate(startCond.op, startCond.value, GDouble(input._1._4), input._1._1)
        }
      case stopCond: StopCondition =>
        input._1._5 match {
          case '*' => applyRegionPredicate(stopCond.op, stopCond.value, GDouble(input._1._4), input._1._1)
          case '+' => applyRegionPredicate(stopCond.op, stopCond.value, GDouble(input._1._4), input._1._1)
          case '-' => applyRegionPredicate(stopCond.op, stopCond.value, GDouble(input._1._3), input._1._1)
        }
      case predicate: RegionCondition.Predicate => applyRegionPredicate(predicate.operator, predicate.value, input._2(predicate.position), input._1._1)
      case region_cond: OR => applyRegionConditionOR(region_cond.first_predicate, region_cond.second_predicate, input)
      case region_cond: AND => applyRegionConditionAND(region_cond.first_predicate, region_cond.second_predicate, input)
      case region_cond: NOT => applyRegionConditionNOT(region_cond.predicate, input)
    }
  }

  //Predicate evaluation methods

  def applyRegionPredicate(operator: REG_OP, value: Any, input: GValue, sampleID: Long): Boolean = {
    operator match {
      case REG_OP.EQ => applyRegionPredicateEQ(value, input, sampleID)
      case REG_OP.NOTEQ => applyRegionPredicateNOTEQ(value, input, sampleID)
      case REG_OP.GT => applyRegionPredicateGT(value, input, sampleID)
      case REG_OP.GTE => applyRegionPredicateGTE(value, input, sampleID)
      case REG_OP.LT => applyRegionPredicateLT(value, input, sampleID)
      case REG_OP.LTE => applyRegionPredicateLTE(value, input, sampleID)
    }
  }

  @throws[SelectFormatException]
  def applyRegionPredicateEQ(value: Any, input: GValue, sampleID: Long): Boolean = {

    value match {
      case metaList: Array[MetaType] =>
        try {
          applyRegionPredicateEQ(castDoubleOrString(metaList.filter(_._1.equals(sampleID))(0)._2._2), input, sampleID)
        } catch {
          case e: IndexOutOfBoundsException => false
        }
      case value: Int => if (input.isInstanceOf[GNull]) false else input.asInstanceOf[GDouble].v.equals(value.toDouble)
      case value: Long => if (input.isInstanceOf[GNull]) false else input.asInstanceOf[GDouble].v.equals(value.toDouble)
      case value: Double => if (input.isInstanceOf[GNull]) false else input.asInstanceOf[GDouble].v.equals(value.toDouble)
      case value: String => input.asInstanceOf[GString].v.equals(value)

    }
  }

  @throws[SelectFormatException]
  def applyRegionPredicateNOTEQ(value: Any, input: GValue, sampleID: Long): Boolean = {
    value match {
      case metaList: Array[MetaType] =>
        try {
          applyRegionPredicateNOTEQ(castDoubleOrString(metaList.filter(_._1.equals(sampleID))(0)._2._2), input, sampleID)
        } catch {
          case e: IndexOutOfBoundsException => false
        }
      case value: Int => if (input.isInstanceOf[GNull]) false else !input.asInstanceOf[GDouble].v.equals(value.toDouble)
      case value: Long => if (input.isInstanceOf[GNull]) false else !(input.asInstanceOf[GDouble].v.equals(value.toDouble))
      case value: Double => if (input.isInstanceOf[GNull]) false else !input.asInstanceOf[GDouble].v.equals(value.toDouble)
      case value: String => !input.asInstanceOf[GString].v.equals(value)
    }
  }

  @throws[SelectFormatException]
  def applyRegionPredicateLT(value: Any, input: GValue, sampleID: Long): Boolean = {
    value match {
      case metaList: Array[MetaType] =>
        try {
          applyRegionPredicateLT(castDoubleOrString(metaList.filter(_._1.equals(sampleID))(0)._2._2), input, sampleID)
        } catch {
          case e: IndexOutOfBoundsException => false
        }
      case value: Int => if (input.isInstanceOf[GNull]) false else input.asInstanceOf[GDouble].v < value.toDouble
      case value: Long => if (input.isInstanceOf[GNull]) false else input.asInstanceOf[GDouble].v < value.toDouble
      case value: Double => if (input.isInstanceOf[GNull]) false else input.asInstanceOf[GDouble].v < value.toDouble
      case value: String =>
        throw SelectFormatException.create("Your SELECT statement cannot be executed: yuo are doing a < comparison between string. Query: " + value + " < " + input)

    }
  }

  @throws[SelectFormatException]
  def applyRegionPredicateLTE(value: Any, input: GValue, sampleID: Long): Boolean = {
    value match {
      case metaList: Array[MetaType] =>
        try {
          applyRegionPredicateLTE(castDoubleOrString(metaList.filter(_._1.equals(sampleID))(0)._2._2), input, sampleID)
        } catch {
          case e: IndexOutOfBoundsException => false
        }

      case value: Int => if (input.isInstanceOf[GNull]) false else input.asInstanceOf[GDouble].v <= value.toDouble
      case value: Long => if (input.isInstanceOf[GNull]) false else input.asInstanceOf[GDouble].v <= value.toDouble
      case value: Double => if (input.isInstanceOf[GNull]) false else input.asInstanceOf[GDouble].v <= value.toDouble
      case value: String =>
        throw SelectFormatException.create("Your SELECT statement cannot be executed: yuo are doing a <= comparison between string. Query: " + value + " <= " + input)
    }
  }

  @throws[SelectFormatException]
  def applyRegionPredicateGT(value: Any, input: GValue, sampleID: Long): Boolean = {
    value match {
      case metaList: Array[MetaType] =>
        try {
          applyRegionPredicateGT(castDoubleOrString(metaList.filter(_._1.equals(sampleID))(0)._2._2), input, sampleID)
        } catch {
          case e: IndexOutOfBoundsException => false
        }
      case value: Int => if (input.isInstanceOf[GNull]) false else input.asInstanceOf[GDouble].v > value.toDouble
      case value: Long => if (input.isInstanceOf[GNull]) false else input.asInstanceOf[GDouble].v > value.toDouble
      case value: Double => if (input.isInstanceOf[GNull]) false else input.asInstanceOf[GDouble].v > value.toDouble
      case value: String =>
        throw SelectFormatException.create("Your SELECT statement cannot be executed: yuo are doing a > comparison between string. Query: " + value + " > " + input)
    }
  }

  @throws[SelectFormatException]
  def applyRegionPredicateGTE(value: Any, input: GValue, sampleID: Long): Boolean = {
    value match {
      case metaList: Array[MetaType] =>
        try {
          applyRegionPredicateGTE(castDoubleOrString(metaList.filter(_._1.equals(sampleID))(0)._2._2.trim.toDouble), input, sampleID)
        } catch {
          case e: IndexOutOfBoundsException => false
        }
      case value: Int => if (input.isInstanceOf[GNull]) false else input.asInstanceOf[GDouble].v >= value.toDouble
      case value: Long => if (input.isInstanceOf[GNull]) false else input.asInstanceOf[GDouble].v >= value.toDouble
      case value: Double => if (input.isInstanceOf[GNull]) false else input.asInstanceOf[GDouble].v >= value.toDouble
      case value: String =>
        throw SelectFormatException.create("Your SELECT statement cannot be executed: yuo are doing a >= comparison between string. Query: " + value + " >= " + input)
    }
  }

  //Composed metacondition evaluation methods

  @throws[SelectFormatException]
  def applyRegionConditionOR(meta1: RegionCondition, meta2: RegionCondition, input: GRECORD): Boolean = {
    applyRegionSelect(meta1, input) || applyRegionSelect(meta2, input)
  }

  @throws[SelectFormatException]
  def applyRegionConditionAND(meta1: RegionCondition, meta2: RegionCondition, input: GRECORD): Boolean = {
    applyRegionSelect(meta1, input) && applyRegionSelect(meta2, input)
  }

  @throws[SelectFormatException]
  def applyRegionConditionNOT(regions: RegionCondition, input: GRECORD): Boolean = {
    !applyRegionSelect(regions, input)
  }

  def castDoubleOrString(value: Any) = {
    try {
      value.toString.trim.toDouble
    } catch {
      case e: Throwable => value.toString
    }
  }

}
