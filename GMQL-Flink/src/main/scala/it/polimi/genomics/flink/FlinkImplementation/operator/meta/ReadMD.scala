package it.polimi.genomics.flink.FlinkImplementation.operator.meta

import it.polimi.genomics.core.DataTypes
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.GMQLLoader
import it.polimi.genomics.flink.FlinkImplementation.reader.{TestingMetaReader, DefaultMetaReader}
import org.apache.flink.api.scala._
import org.slf4j.LoggerFactory

/**
 * Created by michelebertoni on 05/05/15.
 */
object ReadMD {

  final val logger = LoggerFactory.getLogger(this.getClass)

  def apply(paths: List[String], loader: GMQLLoader[Any, Any, Any, Any], labeler : Boolean, env : ExecutionEnvironment, testingIOFormats : Boolean = false) : DataSet[FlinkMetaType] = {

    //logger.warn("Executing ReadMD")

    def parser(x: (Long, String)) = loader.asInstanceOf[GMQLLoader[(Long, String), DataTypes.FlinkRegionType, (Long, String), DataTypes.FlinkMetaType]].meta_parser(x)
    //val paths = List(path+"meta1/", path+"meta2/")
    //val paths2 = paths.map((v) => v + "meta/")
//    println ("Pathes");paths.foreach(println _)
    val ds =
      if(paths.size.equals(0)){
        env.fromElements[FlinkMetaType]()
      } else {
        env.readFile(if(testingIOFormats) {new TestingMetaReader(parser)(paths)}  else { new DefaultMetaReader(parser)(paths)}, paths.head)
      }
//    ds.collect().foreach(println _)

    if(labeler){
      val extracted =
        ds
          .distinct(0)
          .map((v) => (v._1, "GMQL_metafirst_id", v._1.toString()))
      ds.union(extracted)
    } else {
      ds
    }
    //env.createInput[FlinkMetaType](new DefaultMetaReader(parser)(paths))
  }
}
