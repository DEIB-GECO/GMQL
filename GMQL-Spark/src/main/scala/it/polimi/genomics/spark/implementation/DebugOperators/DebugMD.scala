package it.polimi.genomics.spark.implementation.DebugOperators

import it.polimi.genomics.core.DataStructures.{IRDebugMD, IROperator, MetaOperator}
import it.polimi.genomics.core.Debug.EPDAG
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

object DebugMD {

  private final val logger = LoggerFactory.getLogger(this.getClass)

  def apply(executor : GMQLSparkExecutor, input : MetaOperator, debugOperator: IROperator, sc : SparkContext) :  (Float, RDD[(Long, (String, String))]) = {
    logger.info("----------------DebugMD executing..")

    val (operatorStartTime,res) =  executor.implement_md(input, sc)
    val startTime: Float = EPDAG.getCurrentTime

    res.cache().count()

    val epnode = executor.ePDAG.getNodeByDebugOperator(debugOperator)
    epnode.setExecutionStarted(operatorStartTime)


    logger.info("Debugging "+input.getClass.getName)

    epnode.trackOutputReady()

    epnode.trackProfilingStarted()
    logger.info("Profiling...")
    epnode.trackProfilingEnded()


    (startTime, res)

  }


}
