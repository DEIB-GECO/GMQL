package it.polimi.genomics.spark.implementation.RegionsOperators.SelectRegions

import it.polimi.genomics.core.DataStructures.{MetaOperator, RegionOperator}
import it.polimi.genomics.core.DataTypes.GRECORD
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.core.{GMQLSchemaCoordinateSystem, GRecordKey, GValue}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import it.polimi.genomics.spark.implementation.loaders.writeMultiOutputFiles
import it.polimi.genomics.spark.implementation.loaders.writeMultiOutputFiles.RDDMultipleTextOutputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Created by abdulrahman kaitoua on 25/05/15.
  */
object StoreTABRD {
  private final val logger = LoggerFactory.getLogger(StoreTABRD.getClass);
  private final val ENCODING = "UTF-8"

  @throws[SelectFormatException]
  def apply(executor: GMQLSparkExecutor, path: String, value: RegionOperator, associatedMeta: MetaOperator, schema: List[(String, PARSING_TYPE)], coordinateSystem: GMQLSchemaCoordinateSystem.Value, sc: SparkContext): RDD[GRECORD] = {
    val regions = executor.implement_rd(value, sc)
    val meta = executor.implement_md(associatedMeta, sc)

    val MetaOutputPath = path + "/meta/"
    val RegionOutputPath = path + "/files/"

    logger.debug(MetaOutputPath)
    logger.debug(RegionOutputPath)
    logger.debug(regions.toDebugString)
    logger.debug(meta.toDebugString)


    val idsMeta = meta.keys.distinct().collect().sorted

    val newIDS = idsMeta.zipWithIndex.toMap
    val newIDSBroad = sc.broadcast(newIDS)


    val newRegionFileNames = newIDS.map(t => (t._1, "S_%05d.gdm".format(t._2)))
    val newRegionFileNamesBroad = sc.broadcast(newRegionFileNames)


    val regionsPartitioner = new HashPartitioner(newIDS.size) {
      override def getPartition(key: Any): Int = newIDSBroad.value(key.asInstanceOf[GRecordKey].id)
    }


    val metaPartitioner = new HashPartitioner(newIDS.size) {
      override def getPartition(key: Any): Int = newIDSBroad.value(key.asInstanceOf[Long])
    }


    val partitionedRDD: RDD[(GRecordKey, Array[GValue])] =
      regions //.sortBy(s=>s._1) //disabled sorting
        .filter(r => newRegionFileNamesBroad.value.contains(r._1.id))
        .partitionBy(regionsPartitioner)

    //after partition we can destroy the broadcast
    newIDSBroad.unpersist()


    val basedRDD =
      if (coordinateSystem == GMQLSchemaCoordinateSystem.OneBased)
        partitionedRDD
          .map { case (recordKey: GRecordKey, values: Array[GValue]) =>
            (recordKey.copy(start = recordKey.start + 1), values)
          }
      else
        partitionedRDD

    val keyedRDD =
      basedRDD.map {
        case (recordKey: GRecordKey, values: Array[GValue]) =>
          //drop 1 removed the file id
          (newRegionFileNamesBroad.value(recordKey.id), (recordKey.productIterator.drop(1) ++ values).mkString("\t"))
      }

    keyedRDD.saveAsHadoopFile(RegionOutputPath, classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat])

    val metaKeyValue = meta
      .partitionBy(metaPartitioner)
      .mapPartitions({ x: Iterator[(Long, (String, String))] =>
        x.toSeq.sortBy(_._2).toIterator
      }, true)
      .map { x =>
        (newRegionFileNamesBroad.value(x._1) + ".meta", x._2.productIterator.mkString("\t"))
      }

    metaKeyValue.saveAsHadoopFile(MetaOutputPath, classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat])
    writeMultiOutputFiles.fixOutputMetaLocation(MetaOutputPath)

    newRegionFileNamesBroad.unpersist()

    regions
  }
}
