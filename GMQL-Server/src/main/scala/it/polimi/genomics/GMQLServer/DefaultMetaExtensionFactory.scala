package it.polimi.genomics.GMQLServer

import it.polimi.genomics.core.DataStructures.Builtin.MetaExtensionFactory
import it.polimi.genomics.core.DataStructures.MetaAggregate._
import org.slf4j.LoggerFactory

/**
  * Created by Olga Gorlova on 07.06.2017.
  */
object DefaultMetaExtensionFactory extends MetaExtensionFactory {

  val castExc = "GMQL Casting Exception – Could not parse"
  val logger = LoggerFactory.getLogger(this.getClass)

  override def get(dag : MENode, output : String) : MetaExtension = {

    val indexes = extract_indexes(dag).toList

    new MetaExtension {
      override val fun: (Array[Traversable[String]]) => String = make_fun(dag,indexes)
      override val inputAttributeNames: List[String] = indexes
      override val newAttributeName = output
    }
  }

  def extract_indexes(dag : MENode) : Set[String] = {
    dag match {
      case MEADD(x,y) => extract_indexes(x) ++ extract_indexes(y)
      case MESUB(x,y) => extract_indexes(x) ++ extract_indexes(y)
      case MEMUL(x,y) => extract_indexes(x) ++ extract_indexes(y)
      case MEDIV(x,y) => extract_indexes(x) ++ extract_indexes(y)
      case MEName(x) => Set(x.toString)
      case MENegate(x) => extract_indexes(x)
      case MESQRT(x) => extract_indexes(x)
      case _ => Set.empty
    }
  }

  def make_fun(node : MENode, indexes : List[String]) : (Array[Traversable[String]] => String) = {
    node match {
      case MEName(p) => (x: Array[Traversable[String]]) => x(indexes.indexOf(p)).head
      case MEADD(a, b) => (x: Array[Traversable[String]]) => {
        val v1 = castDoubleOrString(make_fun(a, indexes)(x))
        val v2 = castDoubleOrString(make_fun(b, indexes)(x))
        if (v1.isInstanceOf[Double] && v2.isInstanceOf[Double])
          (v1.asInstanceOf[Double] + v2.asInstanceOf[Double]).toString
        else {
//          logger.warn(castExc);
          castExc
        }
      }
      case MESUB(a, b) => (x: Array[Traversable[String]]) => {
        val v1 = castDoubleOrString(make_fun(a, indexes)(x))
        val v2 = castDoubleOrString(make_fun(b, indexes)(x))
        if (v1.isInstanceOf[Double] && v2.isInstanceOf[Double])
          (v1.asInstanceOf[Double] - v2.asInstanceOf[Double]).toString
        else {
//          logger.warn(castExc);
          castExc
        }
      }
      case MEMUL(a, b) => (x: Array[Traversable[String]]) => {
        val v1 = castDoubleOrString(make_fun(a, indexes)(x))
        val v2 = castDoubleOrString(make_fun(b, indexes)(x))
        if (v1.isInstanceOf[Double] && v2.isInstanceOf[Double])
          (v1.asInstanceOf[Double] * v2.asInstanceOf[Double]).toString
        else {
//          logger.warn(castExc);
          castExc
        }
      }
      case MEDIV(a, b) => (x: Array[Traversable[String]]) => {
        val v1 = castDoubleOrString(make_fun(a, indexes)(x))
        val v2 = castDoubleOrString(make_fun(b, indexes)(x))
        if (v1.isInstanceOf[Double] && v2.isInstanceOf[Double])
          (v1.asInstanceOf[Double] / v2.asInstanceOf[Double]).toString
        else {
//          logger.warn(castExc);
          castExc
        }
      }
      case MEFloat(f) => (x: Array[Traversable[String]]) => f.toString
      case MEStringConstant(c) => { (x: Array[Traversable[String]]) => c.toString }
      case MENegate(f) => { x: Array[Traversable[String]] => {
        val v1 = castDoubleOrString(make_fun(f, indexes)(x))
        if (v1.isInstanceOf[Double]) (-(v1.asInstanceOf[Double])).toString
        else {
//          logger.warn(castExc);
          castExc
        }
      }
      }
      case MESQRT(f) => { x: Array[Traversable[String]] => {
        val v1 = castDoubleOrString(make_fun(f, indexes)(x))
        if (v1.isInstanceOf[Double]) (Math.sqrt(v1.asInstanceOf[Double])).toString
        else {
//          logger.warn(castExc);
          castExc
        }
      }
      }
    }
  }

  def castDoubleOrString(value : Any) : Any = {
    try{
      value.toString.toDouble
    } catch {
      case e : Throwable => value.toString
    }
  }
}
