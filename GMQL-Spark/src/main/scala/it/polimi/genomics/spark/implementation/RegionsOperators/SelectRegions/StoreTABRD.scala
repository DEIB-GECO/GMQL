package it.polimi.genomics.spark.implementation.RegionsOperators.SelectRegions

import it.polimi.genomics.core.DataStructures.{MetaOperator, RegionOperator}
import it.polimi.genomics.core.DataTypes.GRECORD
import it.polimi.genomics.core.{GMQLSchemaCoordinateSystem, GRecordKey, GValue}
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import it.polimi.genomics.spark.implementation.loaders.writeMultiOutputFiles
import it.polimi.genomics.spark.implementation.loaders.writeMultiOutputFiles.RDDMultipleTextOutputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.Map

/**
  * Created by abdulrahman kaitoua on 25/05/15.
  */
object StoreTABRD {
  private final val logger = LoggerFactory.getLogger(StoreTABRD.getClass);
  private final val ENCODING = "UTF-8"

  @throws[SelectFormatException]
  def apply(executor: GMQLSparkExecutor, path: String, value: RegionOperator, associatedMeta:MetaOperator, schema : List[(String, PARSING_TYPE)], coordinateSystem: GMQLSchemaCoordinateSystem.Value, sc: SparkContext): RDD[GRECORD] = {
    val regions: RDD[(GRecordKey, Array[GValue])] = executor.implement_rd(value, sc)
    val meta = executor.implement_md(associatedMeta,sc)

//    regions.saveAsObjectFile("/Users/canakoglu/GMQL-sources/gmql_test_ds/test/")

//    val rddReg = sc.objectFile[(GRecordKey, Array[GValue])]("/Users/canakoglu/GMQL-sources/gmql_test_ds/test/")
//    rddReg.collect().foreach(println)

    val MetaOutputPath = path + "/meta/"
    val RegionOutputPath = path + "/exp/"

    logger.debug(MetaOutputPath)
    logger.debug(RegionOutputPath)
    logger.debug(regions.toDebugString)
    logger.debug(meta.toDebugString)


    val outSample = "S"

    val Ids = meta.keys.distinct()
    val newIDS: Map[Long, Long] = Ids.zipWithIndex().collectAsMap()
    val newIDSbroad = sc.broadcast(newIDS)

    val regionsPartitioner = new HashPartitioner(Ids.count.toInt)

    val keyedRDD =
      regions//.sortBy(s=>s._1) //disabled sorting
        .map{x =>
        val newStart = if (coordinateSystem == GMQLSchemaCoordinateSystem.OneBased) (x._1._3 + 1) else x._1._3  //start: 0-based -> 1-based
        (outSample+"_"+ "%05d".format(newIDSbroad.value.get(x._1._1).getOrElse(x._1._1))+".gdm",
        x._1._2 + "\t" + newStart + "\t" + x._1._4 + "\t" + x._1._5 + { if(x._2.length > 0) "\t" + x._2.mkString("\t") else "" })}
          .partitionBy(regionsPartitioner)//.mapPartitions(x=>x.toList.sortBy{s=> val data = s._2.split("\t"); (data(0),data(1).toLong,data(2).toLong)}.iterator)

    keyedRDD.saveAsHadoopFile(RegionOutputPath,classOf[String],classOf[String],classOf[RDDMultipleTextOutputFormat])
//    writeMultiOutputFiles.saveAsMultipleTextFiles(keyedRDD, RegionOutputPath)

    val metaKeyValue = meta.sortBy(x=>(x._1,x._2)).map(x => (outSample+"_"+ "%05d".format(newIDSbroad.value.get(x._1).get) + ".gdm.meta", x._2._1 + "\t" + x._2._2)).partitionBy(regionsPartitioner)

//    writeMultiOutputFiles.saveAsMultipleTextFiles(metaKeyValue, MetaOutputPath)
    metaKeyValue.saveAsHadoopFile(MetaOutputPath,classOf[String],classOf[String],classOf[RDDMultipleTextOutputFormat])
    writeMultiOutputFiles.fixOutputMetaLocation(MetaOutputPath)

    regions
  }
}
