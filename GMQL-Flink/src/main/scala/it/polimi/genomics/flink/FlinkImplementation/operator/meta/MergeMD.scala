package it.polimi.genomics.flink.FlinkImplementation.operator.meta

import it.polimi.genomics.core.DataStructures.{MetaGroupOperator, MetaOperator}
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
 * Created by michelebertoni on 04/06/15.
 */
object MergeMD {

  final val logger = LoggerFactory.getLogger(this.getClass)


  @throws[SelectFormatException]
  def apply(executor : FlinkImplementation, dataset : MetaOperator, groups : Option[MetaGroupOperator], env : ExecutionEnvironment) : DataSet[FlinkMetaType] = {

    //logger.warn("Executing UnionMD")

    val ds : DataSet[FlinkMetaType] =
      executor.implement_md(dataset, env)

    val groupedDs : DataSet[FlinkMetaType] =
      if (groups.isDefined) {
        val grouping = executor.implement_mgd(groups.get, env);
        assignGroups(ds, grouping)
      } else {
        //union of samples
        ds.map((m) => {
          (1L, m._2, m._3)
        })
      }

    val distinctGroupedDs: DataSet[FlinkMetaType] =
      groupedDs.distinct

    distinctGroupedDs
  }


  def assignGroups(dataset : DataSet[FlinkMetaType], grouping : DataSet[FlinkMetaGroupType2]) : DataSet[FlinkMetaType] = {
    dataset.joinWithTiny(grouping).where(0).equalTo(0){
      (m : FlinkMetaType, g : (Long, Long), out : Collector[FlinkMetaType]) => {
        out.collect((g._2, m._2, m._3))
      }
    }
  }

}
