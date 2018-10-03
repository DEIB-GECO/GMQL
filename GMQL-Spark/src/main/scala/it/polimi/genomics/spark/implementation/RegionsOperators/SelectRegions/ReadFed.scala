package it.polimi.genomics.spark.implementation.RegionsOperators.SelectRegions

import it.polimi.genomics.core.DataTypes.{GRECORD, MetaType}
import it.polimi.genomics.spark.implementation.RegionsOperators.SelectRegions.StoreFed.PATH
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object ReadFed {


  def readRegion(path:String, sc: SparkContext): RDD[GRECORD] = {
    val rddReg = sc.objectFile[GRECORD](PATH + "region_" + path)
    rddReg
  }

  def readMeta(path:String, sc: SparkContext): RDD[MetaType] = {
    val rddMeta = sc.objectFile[MetaType](PATH + "meta_" + path)
    rddMeta
  }
}