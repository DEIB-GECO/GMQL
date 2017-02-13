package it.polimi.genomics.flink.FlinkImplementation.operator.region

import it.polimi.genomics.core.DataStructures.IRDataSet
import it.polimi.genomics.core.DataTypes.{FlinkMetaType, FlinkRegionType}
import it.polimi.genomics.core.GMQLLoader
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.flink.FlinkImplementation.reader.{DefaultRegionReader, TestingRegionReader}
import it.polimi.genomics.flink.FlinkImplementation.reader.parser.RepositoryParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
//import it.polimi.genomics.repository.{Utilities => General_Utilities}
//import it.polimi.genomics.repository.FSRepository.{LFSRepository, Utilities => FSR_Utilities}
import org.apache.flink.api.scala._
import org.apache.hadoop.fs.{FileStatus, Path, PathFilter}
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._

/**
 * Created by michelebertoni on 05/05/15.
 */
object ReadRD {


  final val logger = LoggerFactory.getLogger(this.getClass)
  def apply(paths: List[String], loader: GMQLLoader[Any, Any, Any, Any], dummyLoader: Boolean, env: ExecutionEnvironment,testingIOFormats : Boolean = false): DataSet[FlinkRegionType] = {
    //def parser(x: (Long, String)) = loader.asInstanceOf[GMQLLoader[(Long, String), FlinkRegionType, (Long, String), FlinkMetaType]].region_parser(x)
    //env.readFile(new DefaultRegionReader(parser), (path + "exp/"))
    //val paths2 = List(paths(0)+"exp1/", paths(0)+"exp2/")
    //val paths2 = paths.map((v) => v + "exp/")
    if (paths.size.equals(0) || dummyLoader) {
      env.fromElements[FlinkRegionType]()
    } else {
//      val repository = new LFSRepository()
//      val ds = new IRDataSet(paths.head, List[(String,PARSING_TYPE)]().asJava)
      def parser(x: (Long, String)) =
//        if(!testingIOFormats && !paths.isEmpty && repository.DSExists( ds)){
//          val username = if(repository.DSExistsInPublic(ds)) "public" else General_Utilities().USERNAME
//          RepositoryParser(General_Utilities().getSchemaDir(username)+paths.head+".schema").region_parser(x)
//        } else {
          loader.asInstanceOf[GMQLLoader[(Long, String), FlinkRegionType, (Long, String), FlinkMetaType]].region_parser(x)
//        }
      val conf = new Configuration();
      val path = new org.apache.hadoop.fs.Path(paths.head);
      val fs = FileSystem.get(path.toUri(), conf);
      val file = new Path(paths.head)
      val newPathes =
        if(fs.isDirectory(file))
         fs.listStatus(new org.apache.hadoop.fs.Path(paths(0)), new PathFilter {
            override def accept(path: org.apache.hadoop.fs.Path): Boolean = !path.toString.endsWith(".meta")
          }).map(_.getPath.toString).toList
      else
      List[String]()
      env.readFile(if (false) {new TestingRegionReader(parser)(newPathes)}else { new DefaultRegionReader(parser)(paths)}, paths.head)
    }

  }
}