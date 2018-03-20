package it.polimi.genomics.spark.implementation.RegionsOperators

import java.nio.charset.StandardCharsets

import com.google.common.hash.Hashing
import it.polimi.genomics.core.DataStructures.{Feature, GMQLDatasetProfile, IROperator, MetaOperator}
import it.polimi.genomics.core.DataStructures.RegionCondition.RegionCondition
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.GMQLLoader
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path, PathFilter}
import it.polimi.genomics.spark.implementation.loaders.Loaders._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.xml.{Elem, XML}

/**
  * Created by Abdulrahman Kaitoua on 25/05/15.
  */
object SelectIRD {

  private final val logger = LoggerFactory.getLogger(this.getClass);
  var executor: GMQLSparkExecutor = null

  @throws[SelectFormatException]
  def apply(operator: IROperator, executor: GMQLSparkExecutor, regionCondition: Option[RegionCondition], filteredMeta: Option[MetaOperator], loader: GMQLLoader[Any, Any, Any, Any], URIs: List[String], repo: Option[String], sc: SparkContext): RDD[GRECORD] = {

    PredicateRD.executor = executor
    val optimized_reg_cond = if (regionCondition.isDefined) Some(PredicateRD.optimizeConditionTree(regionCondition.get, false, filteredMeta, sc))
    else {
      None
    }
    logger.info("----------------SelectIRD ")

    val conf = new Configuration();
    val path = new org.apache.hadoop.fs.Path(URIs.head);
    val fs = FileSystem.get(path.toUri(), conf);

    val files: List[String] =

      URIs.flatMap { dirInput =>
        val uri = new Path(dirInput)
        if (fs.isDirectory(uri)) {
          fs.listStatus(new Path(dirInput), new PathFilter {
            override def accept(path: Path): Boolean = fs.exists(new Path(path.toString + ".meta"))
          }).map(x => x.getPath.toString).toList;
        } else if (fs.exists(uri)) List(dirInput)
        else None
      }


    val inputURIs: Map[Long, String] = files.map { x =>
      val uri = x //.substring(x.indexOf(":") + 1, x.size).replaceAll("/", "");
      Hashing.md5().newHasher().putString(new Path(uri).getName, StandardCharsets.UTF_8).hash().asLong() -> x
    }.toMap

    val metaIdList = executor.implement_md(filteredMeta.get, sc).keys.distinct.collect
    val selectedURIs = metaIdList.map(x => inputURIs.get(x).get)

    def parser(x: (Long, String)) = loader.asInstanceOf[GMQLLoader[(Long, String), Option[GRECORD], (Long, String), Option[MetaType]]].region_parser(x)

    val result =
    if (selectedURIs.size > 0)
      sc forPath (selectedURIs.mkString(",")) LoadRegionsCombineFiles(parser, PredicateRD.applyRegionSelect, optimized_reg_cond) cache
    else {
      logger.warn("One input select is empty..")
      sc.emptyRDD[GRECORD]
    }


    // Profile loader
    if (operator.requiresOutputProfile && regionCondition.isEmpty) {

      val datasetFolder = (new Path(selectedURIs.head)).getParent.toString
      val profileFile = datasetFolder + "/profile.xml"

      val xmlStream: FSDataInputStream = fs.open(new Path(profileFile))
      val xml = XML.load(xmlStream)

      // generate (id, sample_name_no_format)
      val filteredURIs = inputURIs.filter(x=>metaIdList.contains(x._1))
      val mapping = filteredURIs.map( pair => {
        val sample_path = pair._2
        val fileName = new Path(sample_path) .getName
        (fileName.substring(0, fileName.lastIndexOf(".")),pair._1)
      })

      val profile = GMQLDatasetProfile.fromXML(xml, mapping)

      logger.info("Loaded profile with "+profile.samples.length+" samples.")

      operator.outputProfile = Some(profile)
    }

    result

  }
}
