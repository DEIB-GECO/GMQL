package it.polimi.genomics.spark.implementation.RegionsOperators

import java.io.FileNotFoundException
import java.nio.file.Paths
import javax.xml.bind.JAXBException

import it.polimi.genomics.core.DataTypes
import it.polimi.genomics.core.DataTypes.{GRECORD, MetaType }
import it.polimi.genomics.core.GMQLLoader
import it.polimi.genomics.repository.datasets.GMQLDataSetCollection
import it.polimi.genomics.repository.util.Utilities
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import it.polimi.genomics.spark.implementation.loaders.Loaders._
import org.slf4j.LoggerFactory

/**
 * Created by abdulrahman Kaitoua on 25/05/15.
 */
object ReadRD {

  private final val logger = LoggerFactory.getLogger(SelectRD.getClass);
  def apply(path: List[String], loader : GMQLLoader[Any,Any,Any,Any], sc : SparkContext) : RDD[GRECORD] = {
    def parser(x: (Long, String)) = loader.asInstanceOf[GMQLLoader[(Long, String), Option[GRECORD], (Long, String), Option[MetaType]]].region_parser(x)
    var files =
      if (path.size == 1 && Utilities.getInstance().checkDSNameinRepo(Utilities.USERNAME, path(0))) {
        val username = if(Utilities.getInstance().checkDSNameinPublic(path(0))) "public" else Utilities.USERNAME
        var GMQLDSCol = new GMQLDataSetCollection();
        try {
          GMQLDSCol = GMQLDSCol.parseGMQLDataSetCollection(Paths.get(Utilities.getInstance().RepoDir + username + "/datasets/" + path(0)+".xml"));
          val dataset = GMQLDSCol.getDataSetList.get(0)
          import scala.collection.JavaConverters._
          dataset.getURLs.asScala.map(d =>
            if (Utilities.getInstance().MODE.equals(Utilities.HDFS)) {
              val hdfs = Utilities.getInstance().gethdfsConfiguration().get("fs.defaultFS")
              hdfs + Utilities.getInstance().HDFSRepoDir +username+ "/regions/" + d.geturl
            }
            else { d.geturl}
          )
        } catch {
          case ex: JAXBException => logger.error("DataSet is corrupted"); List[String]()
          case ex: FileNotFoundException => logger.error("DataSet is not Found"); List[String]()
        }
      }else {
        path.flatMap { dirInput =>
          if (new java.io.File(dirInput).isDirectory)
            new java.io.File(dirInput).listFiles.filter(!_.getName.endsWith(".meta")).map(x => x.getPath)
          else List(dirInput)
        }
      }
  sc.forPath(files.mkString(",")).LoadRegionsCombineFiles(parser)

  }
}
