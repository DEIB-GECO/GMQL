package it.polimi.genomics.spark.implementation.DebugOperators

import it.polimi.genomics.core.DataStructures.{IRJoinBy, IROperator, MetaJoinOperator, SomeMetaJoinOperator}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import it.polimi.genomics.spark.implementation.MetaOperators.GroupOperator.MetaJoinMJD2
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

object DebugMJ {

  private final val logger = LoggerFactory.getLogger(this.getClass)

  def apply(executor : GMQLSparkExecutor, input : MetaJoinOperator, debugOperator: IROperator, sc : SparkContext) : RDD[(Long, Array[Long])] = {
    logger.info("----------------DebugMJ executing..")

    val operator = input.asInstanceOf[IRJoinBy]
    val res = MetaJoinMJD2(executor, operator.condition, operator.left_dataset, operator.right_dataset, true, sc).cache()
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
