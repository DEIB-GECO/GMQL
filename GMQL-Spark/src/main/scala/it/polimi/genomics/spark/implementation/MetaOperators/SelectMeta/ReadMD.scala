package it.polimi.genomics.spark.implementation.MetaOperators.SelectMeta

import java.io.FileNotFoundException
import java.nio.file.Paths
import javax.xml.bind.JAXBException

import it.polimi.genomics.core.DataTypes.MetaType
import it.polimi.genomics.core.{DataTypes, GMQLLoader}
import it.polimi.genomics.repository.datasets.GMQLDataSetCollection
import it.polimi.genomics.repository.util.Utilities
import it.polimi.genomics.spark.implementation.loaders.Loaders._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
 * Created by abdulrahman kaitoua on 05/05/15.
 */
object ReadMD {
  private final val logger = LoggerFactory.getLogger(ReadMD.getClass);
  def apply(path: List[String], loader: GMQLLoader[Any, Any, Any, Any], sc : SparkContext) : RDD[MetaType] = {
    logger.info("----------------ReadMD executing..")
    def parser(x: (Long, String)) = loader.asInstanceOf[GMQLLoader[(Long, String), Option[DataTypes.GRECORD], (Long, String), Option[DataTypes.MetaType]]].meta_parser(x)
    var files = if (path.size == 1 && Utilities.getInstance().checkDSNameinRepo(Utilities.USERNAME, path(0))) {
      val username = if(Utilities.getInstance().checkDSNameinPublic(path(0))) "public" else Utilities.USERNAME
      var GMQLDSCol = new GMQLDataSetCollection();
      try {
        GMQLDSCol = GMQLDSCol.parseGMQLDataSetCollection(Paths.get(Utilities.getInstance().RepoDir + username + "/datasets/" + path(0) + ".xml"));
        val dataset = GMQLDSCol.getDataSetList.get(0)
        import scala.collection.JavaConverters._
        dataset.getURLs.asScala.map(d =>if(Utilities.getInstance().MODE.equals("MAPREDUCE")) Utilities.getInstance().HDFSRepoDir+username+"/regions/"+d.geturl+".meta" else d.geturl+".meta")
      } catch {
        case ex: JAXBException => logger.error("DataSet is corrupted"); List[String]()
        case ex: FileNotFoundException => logger.error("DataSet is not Found"); List[String]()
      }
    } else {
      path.flatMap { dirInput =>
        if (new java.io.File(dirInput).isDirectory)
          new java.io.File(dirInput).listFiles.filter(!_.getName.endsWith(".meta")).map(x => x.getPath)
        else List(dirInput)
      }
    }
    val metaPath = files.map(x=>x+".meta").mkString(",")
    sc forPath(metaPath) LoadMetaCombineFiles (parser)
  }
}
