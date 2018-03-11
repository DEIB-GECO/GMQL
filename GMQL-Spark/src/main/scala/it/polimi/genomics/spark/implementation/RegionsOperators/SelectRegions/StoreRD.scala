package it.polimi.genomics.spark.implementation.RegionsOperators

import it.polimi.genomics.core.DataStructures.RegionOperator
import it.polimi.genomics.core.DataTypes.GRECORD
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by abdulrahman kaitoua on 25/05/15.
  */
object StoreRD {
  @throws[SelectFormatException]
  def apply(executor: GMQLSparkExecutor, path: String, value: RegionOperator, sc: SparkContext): RDD[GRECORD] = {
    val input = executor.implement_rd(value, sc)
    input
  }
}

