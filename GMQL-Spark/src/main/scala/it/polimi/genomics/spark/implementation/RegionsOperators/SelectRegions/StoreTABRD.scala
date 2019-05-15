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


    val outSample = "S_"

    val idsMeta = meta.keys.distinct().collect().sorted

    val newIDS = idsMeta.zipWithIndex.toMap
    val newIDSBroad = sc.broadcast(newIDS)


//    val newRegionFileIds: Map[Long, Int] = newIDSBroad.value //.map(t => (t._1, outSample + "%05d".format(t._2) + ".gdm"))

    val newRegionFileNames1 = newIDS.map(t => (t._1, outSample + "%05d".format(t._2) + ".gdm"))
    val newRegionFileNamesBroad = sc.broadcast(newRegionFileNames1)


    //    val newMetaFileNames = newRegionFileNames.map(t => (t._1, t._2 + ".meta"))


    val regionsPartitioner = new HashPartitioner(newIDSBroad.value.size) {
      override def getPartition(key: Any): Int = newIDSBroad.value(key.asInstanceOf[GRecordKey].id) //super.getPartition(key.asInstanceOf[GRecordKey].id)
    }


    val metaPartitioner = new HashPartitioner(newIDSBroad.value.size) {
      override def getPartition(key: Any): Int = newIDSBroad.value(key.asInstanceOf[Long]) //super.getPartition(key.asInstanceOf[GRecordKey].id)
    }


    val keyedRDD: RDD[(String, String)] =
      regions //.sortBy(s=>s._1) //disabled sorting
        .filter(r => newRegionFileNamesBroad.value.contains(r._1.id))
        .partitionBy(regionsPartitioner)
        .map {
          case (recordKey: GRecordKey, values: Array[GValue]) =>
            val newStart =
              if (coordinateSystem == GMQLSchemaCoordinateSystem.OneBased)
                recordKey.start + 1
              else
                recordKey.start //start: 0-based -> 1-based


            val mergedArray = Array(
              recordKey.chrom,
              newStart,
              recordKey.stop,
              recordKey.strand
            ) ++ values

            (newRegionFileNamesBroad.value(recordKey.id), mergedArray.mkString("\t"))
        }



    //.mapPartitions(x=>x.toList.sortBy{s=> val data = s._2.split("\t"); (data(0),data(1).toLong,data(2).toLong)}.iterator)

    keyedRDD.saveAsHadoopFile(RegionOutputPath, classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat])
    //    writeMultiOutputFiles.saveAsMultipleTextFiles(keyedRDD, RegionOutputPath)


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

    regions
  }
}
