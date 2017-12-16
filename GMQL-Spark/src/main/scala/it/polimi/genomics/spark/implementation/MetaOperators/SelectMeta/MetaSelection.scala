package it.polimi.genomics.spark.implementation.MetaOperators.SelectMeta

import it.polimi.genomics.core.DataStructures.MetadataCondition._
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.exception.SelectFormatException
import org.apache.spark.rdd.RDD

trait MetaSelection {
  val ALL = "*"

  //Other usefull methods
  def castDoubleOrString(value : Any) : Any = {
    try{
      value.toString.toDouble
    } catch {
      case e : Throwable => value.toString
    }
  }

  @throws[SelectFormatException]
  def build_set_filter(metadataCondition: MetadataCondition) : (Iterable[(String, String)]) => Boolean = {
    metadataCondition match {
      case metaCondition: Predicate => {
        val predicate = metaCondition.asInstanceOf[Predicate]
        predicate.operator match {
          case META_OP.EQ => buildMetaPredicateEQ(predicate)
          case META_OP.NOTEQ => buildMetaPredicateNOTEQ(predicate)
          case META_OP.GT =>  buildMetaPredicateGT(predicate)
          case META_OP.GTE => buildMetaPredicateGTE(predicate)
          case META_OP.LT =>  buildMetaPredicateLT(predicate)
          case META_OP.LTE =>  buildMetaPredicateLTE(predicate)
        }
      }

      case contain : ContainAttribute => {
        buildContainAttribute(contain.attribute)
      }

      case missing : MissingAttribute => {
        build_set_filter(NOT(ContainAttribute(missing.attribute)))
      }

      case metaCondition: NOT => {
        x:Iterable[(String, String)] => !build_set_filter(metaCondition.predicate)(x)
      }

      case metaCondition: OR => {
        x:Iterable[(String, String)] =>
          build_set_filter(metaCondition.first_predicate)(x) ||
            build_set_filter(metaCondition.second_predicate)(x)
      }

      case metaCondition: AND => {
        x:Iterable[(String, String)] =>
          build_set_filter(metaCondition.first_predicate)(x) &&
            build_set_filter(metaCondition.second_predicate)(x)
      }
    }
  }

  @throws[SelectFormatException]
  def buildMetaPredicateEQ(predicate: Predicate):  Iterable[(String, String)] => Boolean =  {
    castDoubleOrString(predicate.value) match {
      case value: Double =>
        s:Iterable[(String,String)] => s.exists({
          x =>
            (x._1.equals(predicate.attribute_name) || x._1.endsWith("."+predicate.attribute_name)) &&
              (value.equals(ALL) ||
                (try{x._2.toDouble.equals(value)} catch {case _ : Throwable => false})
                )
        })
      case value: String =>
        s:Iterable[(String,String)] => s.exists({
          x =>
            (x._1.equals(predicate.attribute_name) || x._1.endsWith("."+predicate.attribute_name)) &&
              (value.equals(ALL) ||
                x._2.toLowerCase.equals(value.toLowerCase())
                )
        })
    }
  }

  @throws[SelectFormatException]
  def buildMetaPredicateNOTEQ(predicate: Predicate):  Iterable[(String, String)] => Boolean =  {
    castDoubleOrString(predicate.value) match {
      case value: Double =>
        s:Iterable[(String,String)] => s.exists({
          x =>
            (x._1.equals(predicate.attribute_name) || x._1.endsWith("."+predicate.attribute_name)) &&
              (try{!x._2.toDouble.equals(value)} catch {case _ : Throwable => true})
        })
      case value: String =>
        s:Iterable[(String,String)] => s.exists({
          x =>
            (x._1.equals(predicate.attribute_name) || x._1.endsWith("."+predicate.attribute_name)) &&
              !x._2.toLowerCase.equals(value.toLowerCase())
        })
    }
  }

  @throws[SelectFormatException]
  def buildMetaPredicateGT(predicate: Predicate):  Iterable[(String, String)] => Boolean =  {
    castDoubleOrString(predicate.value) match {
      case value: Double =>
        s:Iterable[(String,String)] => s.exists({
          x =>
            (x._1.equals(predicate.attribute_name) || x._1.endsWith("."+predicate.attribute_name)) &&
              (try{x._2.toDouble > value} catch {case _ : Throwable => false})

        })
      case value: String =>
        throw SelectFormatException.create("Your SELECT statement cannot be executed: " +
          "you are doing a > comparison between strings. Attribute = " + predicate.value)
    }
  }

  @throws[SelectFormatException]
  def buildMetaPredicateGTE(predicate: Predicate):  Iterable[(String, String)] => Boolean =  {
    castDoubleOrString(predicate.value) match {
      case value: Double =>
        s:Iterable[(String,String)] => s.exists({
          x =>
            (x._1.equals(predicate.attribute_name) || x._1.endsWith("."+predicate.attribute_name)) &&
              (try{x._2.toDouble >= value} catch {case _ : Throwable => false})

        })
      case value: String =>
        throw SelectFormatException.create("Your SELECT statement cannot be executed: " +
          "you are doing a >= comparison between strings. Attribute = " + predicate.value)

    }
  }

  @throws[SelectFormatException]
  def buildMetaPredicateLT(predicate: Predicate):  Iterable[(String, String)] => Boolean =  {
    castDoubleOrString(predicate.value) match {
      case value: Double =>
        s:Iterable[(String,String)] => s.exists({
          x =>
            (x._1.equals(predicate.attribute_name) || x._1.endsWith("."+predicate.attribute_name)) &&
              (try{x._2.toDouble < value} catch {case _ : Throwable => false})

        })
      case value: String =>
        throw SelectFormatException.create("Your SELECT statement cannot be executed: " +
          "you are doing a < comparison between strings. Attribute = " + predicate.value)

    }
  }

  @throws[SelectFormatException]
  def buildMetaPredicateLTE(predicate: Predicate):  Iterable[(String, String)] => Boolean =  {
    castDoubleOrString(predicate.value) match {
      case value: Double =>
        s:Iterable[(String,String)] => s.exists({
          x =>
            (x._1.equals(predicate.attribute_name) || x._1.endsWith("."+predicate.attribute_name)) &&
              (try{x._2.toDouble <= value} catch {case _ : Throwable => false})

        })
      case value: String =>
        throw SelectFormatException.create("Your SELECT statement cannot be executed: " +
          "you are doing a <= comparison between strings. Attribute = " + predicate.value)

    }
  }

  def buildContainAttribute(name : String) : Iterable[(String, String)] => Boolean = {
    s:Iterable[(String,String)] => s.exists(
      x =>(x._1.equals(name) || x._1.endsWith("."+name)))
  }

}
