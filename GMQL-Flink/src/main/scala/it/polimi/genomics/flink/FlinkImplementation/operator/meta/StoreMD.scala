package it.polimi.genomics.flink.FlinkImplementation.operator.meta

import it.polimi.genomics.core.DataStructures.MetaOperator
import it.polimi.genomics.core.DataTypes.FlinkMetaType
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import org.apache.flink.api.scala._
import org.slf4j.LoggerFactory

/**
 * Created by michelebertoni on 05/05/15.
 */
object StoreMD {
  final val logger = LoggerFactory.getLogger(this.getClass)

  @throws[SelectFormatException]
  def apply(executor : FlinkImplementation, path: String, value: MetaOperator, env : ExecutionEnvironment): DataSet[FlinkMetaType] = {
    //logger.warn("Executing StoreMD")
    executor.implement_md(value, env)
  }
}
