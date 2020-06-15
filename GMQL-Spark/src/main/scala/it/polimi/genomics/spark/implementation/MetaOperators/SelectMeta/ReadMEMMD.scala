package it.polimi.genomics.spark.implementation.MetaOperators.SelectMeta

import it.polimi.genomics.core.DataTypes.MetaType
import it.polimi.genomics.core.Debug.EPDAG
import org.apache.spark.rdd.RDD

/**
  * Created by abdulrahman on 22/05/2017.
  */
object ReadMEMMD {
  def apply (metaRDD:Any): (Float, RDD[MetaType]) = {
    val startTime: Float = EPDAG.getCurrentTime
    (startTime, metaRDD.asInstanceOf[RDD[MetaType]])
  }
}
