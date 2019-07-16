package it.polimi.genomics.spark.implementation.RegionsOperators.SelectRegions

import it.polimi.genomics.core.DataStructures.{MetaOperator, RegionOperator}
import it.polimi.genomics.core.DataTypes.{GRECORD, MetaType}
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

object StoreFed {

  private final val logger = LoggerFactory.getLogger(StoreGTFRD.getClass);
  private final val ENCODING = "UTF-8"


  def storeRegion(executor: GMQLSparkExecutor, path: String, value: RegionOperator, sc: SparkContext): RDD[GRECORD] = {
    val regions = executor.implement_rd(value, sc)

    regions.saveAsObjectFile(path)

    //    val rddReg = sc.objectFile[(GRecordKey, Array[GValue])]("/Users/canakoglu/GMQL-sources/gmql_test_ds/test/")
    //    rddReg.collect().foreach(println)

//    logger.info("storeRegion(saved): " + path)
//    for (i <- 1 to 10) {
//      Thread.sleep(10000)
//      logger.info("storeRegion(waiting): " + (i * 10))
//    }


    regions
  }

  def storeMeta(executor: GMQLSparkExecutor, path: String, value: MetaOperator, sc: SparkContext): RDD[MetaType] = {
    val meta = executor.implement_md(value, sc)
    meta.saveAsObjectFile(path)

    //    val rddReg = sc.objectFile[(GRecordKey, Array[GValue])]("/Users/canakoglu/GMQL-sources/gmql_test_ds/test/")
    //    rddReg.collect().foreach(println)

//    logger.info("storeMeta(saved): " + path)
//    for (i <- 1 to 10) {
//      Thread.sleep(10000)
//      logger.info("storeMeta(waiting): " + (i * 10))
//    }


    meta
  }
}