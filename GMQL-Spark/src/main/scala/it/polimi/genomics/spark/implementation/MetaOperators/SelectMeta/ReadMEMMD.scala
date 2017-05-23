package it.polimi.genomics.spark.implementation.MetaOperators.SelectMeta

import it.polimi.genomics.core.DataTypes.MetaType
import org.apache.spark.rdd.RDD

/**
  * Created by abdulrahman on 22/05/2017.
  */
object ReadMEMMD {
  def apply (metaRDD:Any): RDD[MetaType] = {
    metaRDD.asInstanceOf[RDD[MetaType]]
  }
}
