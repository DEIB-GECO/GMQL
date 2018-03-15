package it.polimi.genomics.spark.implementation.RegionsOperators

import it.polimi.genomics.core.DataStructures.{Feature, GMQLDatasetProfile, IROperator}
import it.polimi.genomics.core.DataTypes.{GRECORD, MetaType}
import it.polimi.genomics.core.GMQLLoader
import it.polimi.genomics.spark.implementation.loaders.Loaders._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.xml.XML

/**
  * Created by abdulrahman Kaitoua on 25/05/15.
  */
object ReadRD {

  private final val logger = LoggerFactory.getLogger(SelectRD.getClass);

  def apply(operator: IROperator , paths: List[String], loader: GMQLLoader[Any, Any, Any, Any], sc: SparkContext): RDD[GRECORD] = {
    def parser(x: (Long, String)) = loader.asInstanceOf[GMQLLoader[(Long, String), Option[GRECORD], (Long, String), Option[MetaType]]].region_parser(x)

    val conf = new Configuration();
    val path = new org.apache.hadoop.fs.Path(paths.head);
    val fs = FileSystem.get(path.toUri(), conf);

    var files = paths.flatMap { dirInput =>
      val file = new Path(dirInput)
      if (fs.isDirectory(file))
        fs.listStatus(file, new PathFilter {
          override def accept(path: Path): Boolean = fs.exists(new Path(path.toString + ".meta"))
        }).map(x => x.getPath.toString).toList;
      else List(dirInput)
    }


    val result = sc.forPath(files.mkString(",")).LoadRegionsCombineFiles(parser)

    // Profile Estimation: load stored profile and filter meta


    if (operator.requiresOutputProfile) {

      val datasetFolder = (new Path(paths.head)).getParent.toString
      val profileFile = datasetFolder + "/profile.xml"

      val file = fs.open(new Path(profileFile))
      val xml = XML.load(file)

      // to do pass the mapping
      val profile = GMQLDatasetProfile.fromXML(xml, Map())

      logger.info("\n\n Resulting Profile has: " + profile.get(Feature.NUM_SAMP) +" samples \n\n" )

      operator.outputProfile = Some(profile)
    }

    result
  }
}
