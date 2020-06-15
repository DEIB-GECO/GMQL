package it.polimi.genomics.spark.implementation.DebugOperators

import it.polimi.genomics.core.DataStructures.{IROperator, RegionOperator}
import it.polimi.genomics.core.DataTypes.GRECORD
import it.polimi.genomics.core.Debug.EPDAG
import it.polimi.genomics.profiling.Profilers.{Feature, Profiler}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

object DebugRD {

  private final val logger = LoggerFactory.getLogger(this.getClass)

  def apply(executor : GMQLSparkExecutor, input : RegionOperator,  debugOperator: IROperator, sc: SparkContext) : (Float, RDD[GRECORD]) = {
    logger.info("----------------DebugRD executing..")

    val (operatorStartTime,res) = executor.implement_rd(input, sc)
    res.cache()

    var startTime: Float = EPDAG.getCurrentTime
    val num_rows = res.count()

    val epnode = executor.ePDAG.getNodeByDebugOperator(debugOperator)
    epnode.setExecutionStarted(operatorStartTime)


    epnode.trackOutputReady()

    // Profile
    if(executor.profileData || epnode.isEntryNode) {
      epnode.trackProfilingStarted()
      println( "Profiling " + input.getClass.getName)
      logger.info("Profiling " + input.getClass.getName)
      val profile = Profiler.profile(res, None, sc)

      epnode.setOutputProfile(profile.stats.toMap)

      epnode.trackProfilingEnded()
    } else {
      println("Skipping profiling for  "+input.getClass.getName)
      logger.info("Skipping profiling for  "+input.getClass.getName)
    }


    logger.info("Debugging "+input.getClass.getName)



    (startTime, res)

  }


}
