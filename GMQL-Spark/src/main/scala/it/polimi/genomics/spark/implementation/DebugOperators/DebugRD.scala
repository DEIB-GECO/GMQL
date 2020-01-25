package it.polimi.genomics.spark.implementation.DebugOperators

import it.polimi.genomics.core.DataStructures.{IROperator, RegionOperator}
import it.polimi.genomics.core.DataTypes.GRECORD
import it.polimi.genomics.profiling.Profilers.{Feature, Profiler}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

object DebugRD {

  private final val logger = LoggerFactory.getLogger(this.getClass)

  def apply(executor : GMQLSparkExecutor, input : RegionOperator,  debugOperator: IROperator, sc: SparkContext) : RDD[GRECORD] = {
    logger.info("----------------DebugRD executing..")

    val res = executor.implement_rd(input, sc).cache()
    val num_rows = res.count()

    val epnode = executor.ePDAG.getNodeByDebugOperator(debugOperator)

    epnode.trackProfilingStarted()

    // Profile
    logger.info("Profiling "+input.getClass.getName)
    val profile = Profiler.profile(res, None, sc)

    epnode.setOutputProfile(profile.stats.toMap)

    epnode.trackProfilingEnded()


    epnode.trackOutputReady()

    logger.info("Debugging "+input.getClass.getName)

    epnode.trackProfilingStarted()
    logger.info("Profiling...")
    epnode.trackProfilingEnded()


    res

  }


}
