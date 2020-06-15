package it.polimi.genomics.spark.implementation.RegionsOperators.SelectRegions

import it.polimi.genomics.core.DataTypes.{GRECORD, MetaType}
import it.polimi.genomics.core.Debug.EPDAG
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object ReadFed {


  def readRegion(path:String, sc: SparkContext): (Float, RDD[GRECORD]) = {
    val startTime: Float = EPDAG.getCurrentTime
    val rddReg = sc.objectFile[GRECORD](path)
    (startTime, rddReg)
  }

  def readMeta(path:String, sc: SparkContext): (Float, RDD[MetaType]) = {
    val startTime: Float = EPDAG.getCurrentTime
    val rddMeta = sc.objectFile[MetaType](path)
    (startTime, rddMeta)
  }
}