package it.polimi.genomics.flink.FlinkImplementation.operator.region

import it.polimi.genomics.core.DataStructures.RegionAggregate.RegionsToMeta
import it.polimi.genomics.core.DataStructures.RegionOperator
import it.polimi.genomics.core.DataTypes.FlinkMetaType
import it.polimi.genomics.core.GValue
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import org.apache.flink.api.scala._
import org.slf4j.LoggerFactory


/**
 * Created by michelebertoni on 07/05/15.
 */
object AggregateRD {
  final val logger = LoggerFactory.getLogger(this.getClass)

  @throws[SelectFormatException]
  def apply(executor : FlinkImplementation, aggregators : List[RegionsToMeta], inputDataset : RegionOperator, env : ExecutionEnvironment) : DataSet[FlinkMetaType] = {

    val input = executor.implement_rd(inputDataset, env)


    val projected : DataSet[(Long, Array[List[GValue]])] = input.map((region) => {
      (region._1, aggregators.foldLeft(new Array[List[GValue]](0))((z, a) => z :+ List(region._6(a.inputIndex))))
    })

    val grouped : DataSet[(Long, Array[List[GValue]])] =
      projected
        .groupBy(0)
        .reduce((a,b) => {
          (a._1, a._2.zip(b._2).map((a : (List[GValue], List[GValue])) => {
            a._1 ++ a._2
          }))
        })

    val res: DataSet[(Long, String, String)] = grouped
      .flatMap((group) => {
        aggregators.zip(group._2).map((aggregated) => {
          (group._1, aggregated._1.newAttributeName, aggregated._1.fun(aggregated._2).toString)
        })
      })

    res

    /*
    executor.implement_rd(inputDataset, env)
      //extract the valute from the array
      .map((v : FlinkRegionType) =>
        (v._1, List(v._6(aggregator.inputIndex)))
      )
      .groupBy(0)
      //concatenate values inside one group
      .reduce((a : (Long, List[GValue]), b : (Long, List[GValue])) => {
        (a._1, a._2 ++ b._2)
      })
      //evaluate aggregations
      .map((v : (Long, List[GValue])) => {
        (v._1, aggregator.newAttributeName, aggregator.fun(v._2).toString)
      })

      */


    /*
    executor.implement_rd(inputDataset, env)
      .map((v : FlinkRegionType) => mutable.HashMap((v._1, v._6(aggregator.inputIndex))))
      .reduce((a : mutable.HashMap[Long, GValue], b : mutable.HashMap[Long, GValue]) => {
        b.foreach((v : (Long, GValue)) => {
          a.get(v._1) match {
            case None => a.put(v._1, v._2)
            case Some(t) => a(v._1) = aggregator.fun(t, v._2)
          }
        })
        a
      })
      .flatMap((l : mutable.HashMap[Long, GValue]) =>
      l.map((v : (Long, GValue)) => (v._1, aggregator.newAttributeName, v._2.toString))
      )
      */
  }

}
