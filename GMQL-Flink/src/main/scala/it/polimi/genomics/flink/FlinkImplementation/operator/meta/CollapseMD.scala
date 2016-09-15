package it.polimi.genomics.flink.FlinkImplementation.operator.meta

import it.polimi.genomics.core.DataStructures.{MetaOperator, MetaGroupOperator}
import it.polimi.genomics.core.DataTypes.{FlinkMetaGroupType2, FlinkMetaType}
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import org.apache.flink.api.scala._
import org.slf4j.LoggerFactory

/**
 * Created by michelebertoni on 27/08/15.
 */
object CollapseMD {
  final val logger = LoggerFactory.getLogger(this.getClass)

  def apply(executor : FlinkImplementation, grouping : Option[MetaGroupOperator], inputDataset : MetaOperator, env : ExecutionEnvironment) : DataSet[FlinkMetaType] = {
    val input = executor.implement_md(inputDataset, env)

    if(grouping.isDefined){
      val groups = executor.implement_mgd(grouping.get, env)
      input.join(groups).where(0).equalTo(0){
        (meta : FlinkMetaType, group : FlinkMetaGroupType2) => (group._2, meta._2, meta._3)
      }
    } else {
      input.map((meta : FlinkMetaType) => (0L, meta._2, meta._3))
    }
  }

}
