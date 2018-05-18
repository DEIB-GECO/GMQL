package it.polimi.genomics.spark.implementation.MetaOperators

import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import it.polimi.genomics.core.DataStructures.GroupMDParameters.Direction.Direction
import it.polimi.genomics.core.DataStructures.GroupMDParameters._
import it.polimi.genomics.core.DataStructures.MetaOperator
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.core.{GDouble, GNull, GString, GValue}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
  * Created by abdulrahman kaitoua on 09/06/15.
  */
object OrderMD {

  private final val logger = LoggerFactory.getLogger(OrderMD.getClass);

  @throws[SelectFormatException]
  def apply(executor: GMQLSparkExecutor, ordering: List[(String, Direction)], newAttribute: String, topParameter: TopParameter, inputDataset: MetaOperator, sc: SparkContext): RDD[(Long, (String, String))] = {

    logger.info("----------------OrderMD executing..")


    val ds: RDD[(Long, (String, String))] =
      executor.implement_md(inputDataset, sc)

    val grouping: Boolean =
      topParameter match {
        case NoTop() => false
        case Top(_) => false
        case TopG(_) => true
        case TopP(_) => false
      }

    val top: Int =
      topParameter match {
        case NoTop() => 0
        case Top(v) => v
        case TopP(v) => v
        case TopG(v) => v
      }

    //extract the key of metadata that will be used in grouping/ordering
    val keys: List[String] =
      ordering.map(_._1)

    //meta data that match the grouping/ordering field
    val metaFilteredDs: Seq[(Long, (String, String))] =
      ds.filter { m =>
        keys.contains(m._2._1)
      }.collect

    val distinctFilteredValues = metaFilteredDs.distinct

    val orderingWithIndex: Map[String, Int] = ordering.zipWithIndex.map(x => (x._1._1, x._2)).toMap

    val SortingAttVallist1 = distinctFilteredValues.groupBy(_._1)
    val SortingAttVallist: Array[(Long, List[(String, GValue)])] = SortingAttVallist1.map { x =>
      val missingAtt = keys.filter { s => val dd = x._2.filter(_._2._1.equals(s)).size; dd <= 0 };
      val misingPair: Array[(Int, String, GValue)] = missingAtt.map(m => (orderingWithIndex.get(m).get, m, GDouble(Double.MinValue))).toArray
      (x._1, (x._2.map { f =>
        val value: GValue = try {
          GDouble(f._2._2.toDouble)
        } catch {
          case e: Throwable => /* println(f._2._2.hashCode);*/ GString(f._2._2)
        }
        (orderingWithIndex.get(f._2._1).get, f._2._1, value)
      }.toArray ++ misingPair).toList
        .sortBy(b => b._1)
        .map(f => (f._2, f._3)))
    }.toArray


    var index = -1
    val valuesOrdering: Ordering[Array[GValue]] = ordering.map(x => x._2 match {
      case Direction.ASC => /*println(x);*/ index = index + 1; orderByColumn(index)
      case Direction.DESC => /*println(x);*/ index = index + 1; orderByColumn(index).reverse
    }).reduceLeft((res, x) => res orElse x)


    //for each sample for each key take the first value that will be used for grouping/sorting
    //SampleID, List[metavalues to be used in grouping/ordering ordered as requested in input]
    val valueList: Map[Long, List[String]] =
    metaFilteredDs
      .groupBy(_._1)
      .map { g =>
        keys.foldLeft((g._1, List(): List[String])) { (z, k) =>

          val matchedValues: Seq[(Long, (String, String))] = g._2.filter(m => m._2._1.equals(k))

          val head: String =
            if (matchedValues.isEmpty) {
              null
            } else {
              ordering.filter(o => o._1.equals(matchedValues.head._2._1)).head._2 match {
                case Direction.ASC => matchedValues.sortWith((a, b) => a._2._2.compareTo(b._2._2) < 0).head._2._2
                case Direction.DESC => matchedValues.sortWith((a, b) => a._2._2.compareTo(b._2._2) > 0).head._2._2
              }
            }

          (z._1, z._2 :+ head)

        }
      }


    val valueListBoth: Map[Long, List[GValue]] = valueList.map { g =>
      val inputs = g._2.map { inp =>
        Try(inp.toDouble) match {
          case Success(v) => GDouble(v)
          case Failure(_) => Try(inp.toString) match {
            case Success(v) => GString(v)
            case Failure(_) => GNull()
          }
        }
      }
      (g._1, inputs)
    }


    //list of meta data that will be added
    //if grouping is defined there are 2 meta tuple for each sample
    //they are in the form
    //sampleID newAttribute [ position | positionInGroup ]
    //sampleID newAttribute_group groupID
    val sortedTop: List[(Long, (String, String))] =
    if (grouping) {
      //Grouping group by the first n-1 ordering attributes and then selectes the top k
      val sortedGroups1 = SortingAttVallist.groupBy { record =>
        Hashing.md5.newHasher.putString(record._2.map(_._2).toList.init.mkString("§"), Charsets.UTF_8).hash.asLong
      }.map(s => (s._1, s._2.sortBy((sort: (Long, List[(String, GValue)])) => sort._2.map(_._2).toArray[GValue])(valuesOrdering)))
      /*val percentages1 = if (topParameter.isInstanceOf[TopP]) {
        sortedGroups1.map(x => (x._1, x._2.size * top / 100))
      } else HashMap[Long, Int]()*/

      sortedGroups1.flatMap { g =>
        //TOPG
        val gFiltered: List[Long] =
          if (top == 0) {
            g._2.map(_._1).toList
          } else {
            g._2.map(_._1).take(top /*percentages1.getOrElse(g._1, top)*/).toList
          }
        //create metadata
        assignPosition(Some(g._1), gFiltered, 1, newAttribute, List())
      }.toList

    } else {
      val comparator: ((Long, List[GValue]), (Long, List[GValue])) => Boolean =
        metaSampleComparator(ordering)

      val sortedSamples: List[Long] = valueListBoth.toList.sortWith { (a, b) => comparator(a, b) }.map(_._1)

      //TOPP
      val percentages = if (topParameter.isInstanceOf[TopP]) {
        Some(sortedSamples.size * top / 100)
      } else None

      //TOP
      val filteredSortedSamples: List[Long] =
        if (top != 0)
          sortedSamples.take(percentages.getOrElse(top))
        else sortedSamples

      //create metadata
      assignPosition(None, filteredSortedSamples, 1, newAttribute, List())
    }

    //extract id of resulting set
    //if top is applied it is a subset of original set
    val filteredId: List[Long] = sortedTop.map(_._1).distinct

    //filter input dataset by id
    val topDs: RDD[(Long, (String, String))] =
      ds.filter((m) => filteredId.contains(m._1))

    //Create the dataset, merge with input and return as output
    topDs.union(sc.parallelize(sortedTop))

  }

  /**
    * Creates 1 or 2 metadata line for each sample:
    * one with the groupID (if defined)
    * one with the position in the group
    *
    * @param groupId      groupId
    * @param list         ordered list of sample
    * @param step         position
    * @param newAttribute new attribute name for position, group attribute will be called newAttribute_groupId
    * @param acc          recursive accumulator
    * @return Seq of meta data
    */
  def assignPosition(groupId: Option[Long], list: List[(Long)], step: Int, newAttribute: String, acc: List[(Long, (String, String))]): List[(Long, (String, String))] = {
    if (list.size.equals(0)) {
      acc
    } else {
      if (groupId.isDefined) {
        assignPosition(groupId, list.tail, step + 1, newAttribute,
          acc :+
            (list.head, (newAttribute, step.toString)) :+
            (list.head, (newAttribute + "_groupId", groupId.get.toString))
        )
      } else {
        assignPosition(groupId, list.tail, step + 1, newAttribute,
          acc :+
            (list.head, (newAttribute, step.toString))
        )
      }
    }
  }

  /////////////////////
  //comparator
  /////////////////////

  //true if a < b
  def metaSampleComparator(ordering: List[(String, Direction)])(a: (Long, List[GValue]), b: (Long, List[GValue])): Boolean = {
    val res: Option[Boolean] = comparatorHelper(ordering, a, b)
    // if res is not defined then "a == b", so (a<b) is false
    res.getOrElse(false)
  }


  //true if a < b
  def comparatorHelper(ordering: List[(String, Direction)], a: (Long, List[GValue]), b: (Long, List[GValue])): Option[Boolean] = {
    if (ordering.isEmpty) {
      None
    } else {
      val ord = ordering.head
      val aVal = a._2.head
      val bVal = b._2.head

      if (aVal.equals(bVal)) {
        comparatorHelper(ordering.tail, (a._1, a._2.tail), (b._1, b._2.tail))
      } else {
        ord._2 match {
          case Direction.ASC => Some(aVal.compareTo(bVal) < 0)
          case Direction.DESC => Some(aVal.compareTo(bVal) > 0)
        }
      }
    }
  }

  // Functions and objects needed for sorting by multi values.
  def orderByColumn(col: Int) = Ordering.by { ar: Array[GValue] => ar(col) }

  final class CompositeOrdering[T](val ord1: Ordering[T], val ord2: Ordering[T]) extends Ordering[T] {
    def compare(x: T, y: T) = {
      val comp = ord1.compare(x, y)
      if (comp != 0) comp else ord2.compare(x, y)
    }
  }

  object CompositeOrdering {
    def apply[T](orderings: Ordering[T]*) = orderings reduceLeft (_ orElse _)
  }

  implicit class OrderingOps[T](val ord: Ordering[T]) extends AnyVal {
    def orElse(ord2: Ordering[T]) = new CompositeOrdering[T](ord, ord2)
  }

}
