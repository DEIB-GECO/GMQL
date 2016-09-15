package it.polimi.genomics.flink.FlinkImplementation.operator.meta

import it.polimi.genomics.core.DataStructures.{MetaOperator, RegionOperator}
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import org.apache.flink.api.scala._
import org.slf4j.LoggerFactory

/**
 * Created by michelebertoni on 05/05/15.
 */
object PurgeMD {

  final val logger = LoggerFactory.getLogger(this.getClass)

  @throws[SelectFormatException]
  def apply(executor : FlinkImplementation, regionDataset : RegionOperator, inputDataset : MetaOperator, env : ExecutionEnvironment) : DataSet[FlinkMetaType] = {

    //logger.warn("Executing PurgeMD")

    val input = executor.implement_md(inputDataset, env)
    val metaIdList = executor.implement_rd(regionDataset, env).distinct(0).map(_._1).collect
    input.filter((a : FlinkMetaType) => metaIdList.contains(a._1))
  }
}
