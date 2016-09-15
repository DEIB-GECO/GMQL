package it.polimi.genomics.flink.FlinkImplementation.operator.region

import it.polimi.genomics.core.DataTypes.{FlinkMetaType, FlinkRegionType}
import it.polimi.genomics.core.GMQLLoader
import it.polimi.genomics.flink.FlinkImplementation.reader.{TestingRegionReader, DefaultRegionReader}
import it.polimi.genomics.flink.FlinkImplementation.reader.parser.RepositoryParser
import it.polimi.genomics.repository.util.Utilities
import org.apache.flink.api.scala._
import org.apache.hadoop.fs.{FileStatus, PathFilter, Path}
import org.slf4j.LoggerFactory

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
      def parser(x: (Long, String)) =
        if(!testingIOFormats && !paths.isEmpty && Utilities.getInstance().checkDSNameinRepo(Utilities.USERNAME, paths.head)){
          val username = if(Utilities.getInstance().checkDSNameinPublic(paths.head)) "public" else Utilities.USERNAME
          RepositoryParser(Utilities.getInstance().RepoDir+username+"/schema/"+paths.head+".schema").region_parser(x)
        } else {
          loader.asInstanceOf[GMQLLoader[(Long, String), FlinkRegionType, (Long, String), FlinkMetaType]].region_parser(x)
        }
      val newPathes =
        if(Utilities.getInstance.getFileSystem.isDirectory(new Path(paths(0))))
         Utilities.getInstance.getFileSystem.listStatus(new org.apache.hadoop.fs.Path(paths(0)), new PathFilter {
            override def accept(path: org.apache.hadoop.fs.Path): Boolean = !path.toString.endsWith(".meta")
          }).map(_.getPath.toString).toList
      else
      List[String]()
      env.readFile(if (false) {new TestingRegionReader(parser)(newPathes)}else { new DefaultRegionReader(parser)(paths)}, paths.head)
    }

  }
}