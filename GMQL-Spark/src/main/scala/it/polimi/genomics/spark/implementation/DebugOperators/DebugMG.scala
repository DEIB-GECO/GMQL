package it.polimi.genomics.spark.implementation.DebugOperators

import it.polimi.genomics.core.DataStructures.{IROperator, MetaGroupOperator}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

object DebugMG {

  private final val logger = LoggerFactory.getLogger(this.getClass)


  def apply(executor : GMQLSparkExecutor, input : MetaGroupOperator,  debugOperator: IROperator, sc : SparkContext) : RDD[(Long, Long)]  = {
    logger.info("----------------DebugMG executing..")

    val res = executor.implement_mgd(input, sc).cache()
    res.count()

    logger.info("Debugging "+input.getClass.getName)

    val epnode = executor.ePDAG.getNodeByDebugOperator(debugOperator)

    epnode.trackOutputReady()

    epnode.trackProfilingStarted()
    logger.info("Profiling...")
    epnode.trackProfilingEnded()

    res
  }


}
