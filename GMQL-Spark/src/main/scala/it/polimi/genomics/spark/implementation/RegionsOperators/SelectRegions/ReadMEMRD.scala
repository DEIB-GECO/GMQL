package it.polimi.genomics.spark.implementation.RegionsOperators.SelectRegions

import it.polimi.genomics.core.DataStructures.IROperator
import it.polimi.genomics.core.DataTypes.GRECORD
import org.apache.spark.rdd.RDD

/**
  * Created by abdulrahman on 22/05/2017.
  */
object ReadMEMRD {
  def apply(operator: IROperator, regionDS:Any): RDD[GRECORD] = {
    regionDS.asInstanceOf[RDD[GRECORD]]

  }
}
