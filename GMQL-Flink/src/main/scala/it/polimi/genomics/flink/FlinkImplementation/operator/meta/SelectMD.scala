package it.polimi.genomics.flink.FlinkImplementation.operator.meta

import it.polimi.genomics.core.DataStructures.MetaOperator
import it.polimi.genomics.core.DataStructures.MetadataCondition._
import it.polimi.genomics.core.DataTypes.FlinkMetaType
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
 * Created by michelebertoni on 05/05/15.
 */
object SelectMD {

  final val logger = LoggerFactory.getLogger(this.getClass)

  @throws[SelectFormatException]
  def apply(executor : FlinkImplementation, metaCondition: MetadataCondition, inputDataset: MetaOperator, env : ExecutionEnvironment) : DataSet[FlinkMetaType] = {

    //logger.warn("Executing SelectMD")

    val input = executor.implement_md(inputDataset, env)
    input.join(applyMetaSelect(metaCondition, input, false)).where(0).equalTo(0){
      (a,b) => (a)
    }
  }

  @throws[SelectFormatException]
  def applyMetaSelect(metaCondition: MetadataCondition, input: DataSet[FlinkMetaType], not: Boolean): DataSet[Long] = {
    metaCondition match {
      case metaCondition: Predicate => {
        val predicate = metaCondition.asInstanceOf[Predicate]
        predicate.operator match {
          case META_OP.EQ => {
            applyMetaPredicateEQ(predicate, input)
            /*
            if (not) {
              //!EQ = NOTEQ
              applyMetaPredicateNOTEQ(predicate, input)
            } else {
              //EQ
              applyMetaPredicateEQ(predicate, input)
            }
            */
          }
          case META_OP.NOTEQ => {
            applyMetaPredicateNOTEQ(predicate, input)
            /*
            if (not) {
              //!NOTEQ = EQ
              applyMetaPredicateEQ(predicate, input)
            } else {
              //NOTEQ
              applyMetaPredicateNOTEQ(predicate, input)
            }
            */
          }
          case META_OP.GT => {
            applyMetaPredicateGT(predicate, input)
            /*
            if (not) {
              //!GT = LTE
              applyMetaPredicateLTE(predicate, input)
            } else {
              //GT
              applyMetaPredicateGT(predicate, input)
            }
            */
          }
          case META_OP.GTE => {
            applyMetaPredicateGTE(predicate, input)
            /*
            if (not) {
              //!GTE = LT
              applyMetaPredicateLT(predicate, input)
            } else {
              //GTE
              applyMetaPredicateGTE(predicate, input)
            }
            */
          }
          case META_OP.LT => {
            applyMetaPredicateLT(predicate, input)
            /*
            if (not) {
              //!LT = GTE
              applyMetaPredicateGTE(predicate, input)
            } else {
              //LT
              applyMetaPredicateLT(predicate, input)
            }
            */
          }
          case META_OP.LTE => {
            applyMetaPredicateLTE(predicate, input)
            /*
            if (not) {
              //!LTE = GT
              applyMetaPredicateGT(predicate, input)
            } else {
              //LTE
              applyMetaPredicateLTE(predicate, input)
            }
            */
          }
        }
      }

      case metaCondition: NOT => {
        // OLD
        // applyMetaSelect(metaCondition.predicate, input, !not)
        val subRes = applyMetaSelect(metaCondition.predicate, input, not)
        // take the complete list of ids
        // take the ids that satisfies son condition
        // take the difference of the two set
        input
          .coGroup(subRes).where(0).equalTo(0){
          (left, right, out : Collector[Long]) => {
            if(right.size.equals(0)){
              out.collect(left.next()._1)
            }
          }
        }
      }

      case metaCondition: OR => {
        applyMetaConditionOR(metaCondition.first_predicate, metaCondition.second_predicate, input, not)
        /*
        if (not) {
          //!OR = AND
          applyMetaConditionAND(metaCondition.first_predicate, metaCondition.second_predicate, input, not)
        } else {
          //OR
          applyMetaConditionOR(metaCondition.first_predicate, metaCondition.second_predicate, input, not)
        }
        */
      }

      case metaCondition: AND => {
        applyMetaConditionAND(metaCondition.first_predicate, metaCondition.second_predicate, input, not)
        /*
        if (not) {
          //!AND = OR
          applyMetaConditionOR(metaCondition.first_predicate, metaCondition.second_predicate, input, not)
        } else {
          //AND
          applyMetaConditionAND(metaCondition.first_predicate, metaCondition.second_predicate, input, not)
        }
        */
      }
    }
  }





  //Predicate evaluation methods

  @throws[SelectFormatException]
  def applyMetaPredicateEQ(predicate: Predicate, input: DataSet[FlinkMetaType]): DataSet[Long] = {
    castDoubleOrString(predicate.value) match {
      case value: Double => {
        input.filter(_._2.equals(predicate.attribute_name)).filter((a : FlinkMetaType) => {
          try{
            a._3.toDouble.equals(value)
          } catch {
            case _ : Throwable => false
          }
        })
        .map(_._1)
      }
      case value: String => {
        input
          .filter((v) => v._2.toString.equals(predicate.attribute_name) && v._3.toString.equals(value) )
          .map(_._1)
      }
    }
  }

  @throws[SelectFormatException]
  def applyMetaPredicateNOTEQ(predicate: Predicate, input: DataSet[FlinkMetaType]): DataSet[Long] = {
    castDoubleOrString(predicate.value) match {
      case value: Double => {
        input.filter(_._2.equals(predicate.attribute_name)).filter((a : FlinkMetaType) => {
          try{
            !a._3.toDouble.equals(value)
          } catch {
            case _ : Throwable => false
          }
        })
        .map(_._1)
      }
      case value: String => {
        input
          .filter((v) => v._2.equals(predicate.attribute_name) && !v._3.toString.equals(value))
          .map(_._1)
      }
    }
  }

  @throws[SelectFormatException]
  def applyMetaPredicateLT(predicate: Predicate, input: DataSet[FlinkMetaType]): DataSet[Long] = {
    castDoubleOrString(predicate.value) match {
      case value: Double => {
        input.filter(_._2.equals(predicate.attribute_name)).filter((a : FlinkMetaType) => {
          try{
            a._3.toDouble < value
          } catch {
            case _ : Throwable => false
          }
        })
        .map(_._1)
      }
      case value: String => {
        throw SelectFormatException.create("Your SELECT statement cannot be executed: yuo are doing a < comparison between string. Attribute = " + predicate.value)
      }
    }
  }

  @throws[SelectFormatException]
  def applyMetaPredicateLTE(predicate: Predicate, input: DataSet[FlinkMetaType]): DataSet[Long] = {
    castDoubleOrString(predicate.value) match {
      case value: Double => {
        input.filter(_._2.equals(predicate.attribute_name)).filter((a : FlinkMetaType) => {
          try{
            a._3.toDouble <= value
          } catch {
            case _ : Throwable => false
          }
        })
        .map(_._1)
      }
      case value: String => {
        throw SelectFormatException.create("Your SELECT statement cannot be executed: yuo are doing a <= comparison between string. Attribute = " + predicate.value)
      }
    }
  }

  @throws[SelectFormatException]
  def applyMetaPredicateGT(predicate: Predicate, input: DataSet[FlinkMetaType]): DataSet[Long] = {
    castDoubleOrString(predicate.value) match {
      case value : Double => {
        input.filter(_._2.equals(predicate.attribute_name)).filter((a : FlinkMetaType) => {
          try{
            a._3.toDouble > value
          } catch {
            case _ : Throwable => false
          }
        })
        .map(_._1)
      }
      case value : String => {
        throw SelectFormatException.create("Your SELECT statement cannot be executed: yuo are doing a > comparison between string. Attribute = " + predicate.value)
      }
    }
  }

  @throws[SelectFormatException]
  def applyMetaPredicateGTE(predicate: Predicate, input: DataSet[FlinkMetaType]): DataSet[Long] = {
    castDoubleOrString(predicate.value) match {
      case value : Double => {
        input.filter(_._2.equals(predicate.attribute_name)).filter((a : FlinkMetaType) => {
          try{
            a._3.toDouble >= value
          } catch {
            case _ : Throwable => false
          }
        })
        .map(_._1)
      }

      case value : String => {
        throw SelectFormatException.create("Your SELECT statement cannot be executed: yuo are doing a >= comparison between string. Attribute = " + predicate.value)
      }
    }
  }






  //Composed metacondition evaluation methods

  @throws[SelectFormatException]
  def applyMetaConditionOR(meta1: MetadataCondition, meta2: MetadataCondition, input: DataSet[FlinkMetaType], not: Boolean): DataSet[Long] = {
    applyMetaSelect(meta1, input, not).union(applyMetaSelect(meta2, input, not))
  }

  @throws[SelectFormatException]
  def applyMetaConditionAND(meta1: MetadataCondition, meta2: MetadataCondition, input: DataSet[FlinkMetaType], not: Boolean): DataSet[Long] = {
    applyMetaSelect(meta1, input, not).join(applyMetaSelect(meta2, input, not)).where(0).equalTo(0) {
      (a, b) => (a)
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
}
