package it.polimi.genomics.flink.FlinkImplementation.operator.metaJoin

import it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition
import it.polimi.genomics.core.DataStructures.MetaOperator
import it.polimi.genomics.core.DataTypes.{FlinkMetaJoinType2, FlinkMetaType}
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import org.apache.flink.api.scala._
import org.apache.flink.hadoop.shaded.com.google.common.base.Charsets
import org.apache.flink.hadoop.shaded.com.google.common.hash.Hashing
import org.slf4j.LoggerFactory

import scala.collection.mutable


/**
 * Created by michelebertoni on 13/05/15.
 */
object MetaJoinMJD2 {

  final val logger = LoggerFactory.getLogger(this.getClass)


  //val hf : HashFunction = Hashing.sha256()

  @throws[SelectFormatException]
  def apply(executor : FlinkImplementation, condition :  MetaJoinCondition, leftDataset : MetaOperator, rightDataset : MetaOperator, env : ExecutionEnvironment) : DataSet[FlinkMetaJoinType2] = {


    val dataset: DataSet[(Long, String, String)] =
      executor.implement_md(leftDataset, env)
        .union(
          executor.implement_md(rightDataset, env)
        )
        .filter((v : FlinkMetaType) => condition.attributes.contains(v._2))

    env.fromCollection({
      val in = dataset.collect

      //PURE SCALA
      val groupIdWithSample = in.map((t: FlinkMetaType) => (t._1, mutable.HashMap((t._2, List(t._3)))))
        .groupBy(_._1).toList
        .flatMap((l) => {

          val p = l._2.reduce((a: (Long, mutable.HashMap[String, List[String]]), b: (Long, mutable.HashMap[String, List[String]])) => {
            b._2.foreach((v: (String, List[String])) => {
              a._2.get(v._1) match {
                case None => a._2.put(v._1, v._2)
                case Some(l) => a._2(v._1) = l ++ v._2
              }
            })
            a
          })

          if (p._2.size.equals(condition.attributes.size)) {
            //TODO hasher
            //splatter(p._2.toList.sortBy(_._1)).map(v => (hf.newHasher().putString(v, Charsets.UTF_8).hash.asLong, List(p._1)))
            //splatter(p._2.toList.sortBy(_._1)).map(v => (v.hashCode.toLong, List(p._1)))
            splatter(p._2.toList.sortBy(_._1)).map(v => (Hashing.md5().hashString(v.toString, Charsets.UTF_8).asLong(), List(p._1)))
          } else {
            None
          }

        })

      val groupIdWithListOfSamples = groupIdWithSample.groupBy(_._1)
        .map((l) => {
          l._2.reduce((a: (FlinkMetaJoinType2), b: (FlinkMetaJoinType2)) => {
            (a._1, a._2 ++ b._2)
          })
        })

      val sampleIdWithListOfGroups = groupIdWithListOfSamples.flatMap((t) => {
          if (t._2.size > 1) {
            t._2.map((sampleId: Long) => (sampleId, List(t._1)))
          } else {
            None
          }
        })
        .groupBy(_._1)
          .map((t) => {
          t._2.reduce((a: (FlinkMetaJoinType2), b: (FlinkMetaJoinType2)) => (a._1, a._2 ++ b._2))
        })
        .toList
      //END PURE SCALA

      logger.error("--------------------- METAJOINMJD sampleIdWithListOfGroups " + sampleIdWithListOfGroups)
      sampleIdWithListOfGroups
    })


    //env.fromCollection(List((-92839856L,List(1598L)), (-91916335L,List(1598L))))
    //env.fromCollection(List((6232373565593090030L,List(-1524854089060922635L)), (794754189817364373L,List(-1524854089060922635L))))



    /*
    val sampleIdWithMapOfAttributesFiltered : DataSet[(Long, List[(String, List[String])])] =
      dataset
        .map((t : FlinkMetaType) => (t._1, mutable.HashMap((t._2, List(t._3)))))
        .groupBy(_._1)
        .reduce((a : (Long, mutable.HashMap[String, List[String]]), b : (Long, mutable.HashMap[String, List[String]])) => {
          b._2.foreach((v : (String, List[String])) => {
            a._2.get(v._1) match {
              case None => a._2.put(v._1, v._2)
              case Some(l) => a._2(v._1) = l ++ v._2
            }
          })
          a
        })
        .flatMap((p : (Long,  mutable.HashMap[String, List[String]]), out : Collector[(Long, List[(String, List[String])])]) => {
          if(p._2.size.equals(condition.attributes.size)){
            out.collect((p._1, p._2.toList))
          }
        })
    //(sampleId, Map(attribute->List(Value))

    val groupIdWithSample : DataSet[FlinkMetaJoinType] =
      sampleIdWithMapOfAttributesFiltered
        .flatMap((v1 : (Long, List[(String, List[String])])) => {
          splatter(v1._2.sortBy(_._1)).map(v => (hf.newHasher().putString(v, Charsets.UTF_8).hash.asLong, List(v1._1)))
        })
    //(groupId, List(sample))

    val groupIdWithListOfSamples : DataSet[FlinkMetaJoinType] =
      groupIdWithSample
        .groupBy(_._1)
        .reduce((a : (FlinkMetaJoinType),b : (FlinkMetaJoinType)) => (a._1, a._2 ++ b._2)).filter(_._2.size > 1)
    //(groupId, List(samples))

    val sampleIdWithListOfGroups : DataSet[FlinkMetaJoinType] =
      groupIdWithListOfSamples
        .flatMap((t : (FlinkMetaJoinType)) => t._2.map((sampleId : Long) => (sampleId, List(t._1)))).groupBy(0)
        .reduce((a : (FlinkMetaJoinType), b : (FlinkMetaJoinType)) => (a._1, a._2 ++ b._2))
    //(sampleId, List(groups)

    //logger.error("----------- debug " + countElementInDataset(sampleIdWithListOfGroups))
    sampleIdWithListOfGroups
    */
  }


  def splatter(grid : List[(String, List[String])]) : List[String] = {
    grid.size match {
      case 1 => grid(0)._2
      case _ => concatenator(grid(0)._2, splatter(grid.drop(0)))
    }
  }

  def concatenator(l1 : List[String], l2 : List[String]) : List[String] = {
    l1.flatMap(s => l2.map(_ + s))
  }



}
