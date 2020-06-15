package it.polimi.genomics.spark.implementation.RegionsOperators.SelectRegions

import it.polimi.genomics.core.DataTypes.GRECORD
import it.polimi.genomics.core.Debug.EPDAG
import org.apache.spark.rdd.RDD

/**
  * Created by abdulrahman on 22/05/2017.
  */
object ReadMEMRD {
  def apply(regionDS:Any): (Float, RDD[GRECORD]) = {
    val startTime: Float = EPDAG.getCurrentTime
    (startTime, regionDS.asInstanceOf[RDD[GRECORD]])

  }
}
