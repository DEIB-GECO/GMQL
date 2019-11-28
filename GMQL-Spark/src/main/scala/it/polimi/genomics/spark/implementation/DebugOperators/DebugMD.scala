package it.polimi.genomics.spark.implementation.DebugOperators

import it.polimi.genomics.core.DataStructures.{IRDebugMD, IROperator, MetaOperator}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

object DebugMD {

  private final val logger = LoggerFactory.getLogger(this.getClass)

  def apply(executor : GMQLSparkExecutor, input : MetaOperator, debugOperator: IROperator, sc : SparkContext) :  RDD[(Long, (String, String))] = {
    logger.info("----------------DebugMD executing..")

    val res =  executor.implement_md(input, sc).cache()
    res.count()

    val epnode = executor.ePDAG.getNodeByDebugOperator(debugOperator)
    logger.info("Debugging "+input.getClass.getName)

    epnode.trackOutputReady()

    epnode.trackProfilingStarted()
    logger.info("Profiling...")
    epnode.trackProfilingEnded()


    res

  }


}
