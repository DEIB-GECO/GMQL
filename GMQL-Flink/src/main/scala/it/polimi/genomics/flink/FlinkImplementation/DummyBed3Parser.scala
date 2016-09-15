package it.polimi.genomics.flink.FlinkImplementation

import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.{GValue, DataTypes, GMQLLoader}

/**
 * Created by pietro on 10/03/15.
 */
object DummyBed3Parser extends  GMQLLoader[(Long,String), DataTypes.FlinkRegionType, (Long,String), DataTypes.FlinkMetaType] with java.io.Serializable{

  override def meta_parser(t : (Long, String)) : DataTypes.FlinkMetaType = {
    val s = t._2 split "\t"
    (t._1, s(0), s(1))
  }

  override def region_parser(t : (Long, String)) : DataTypes.FlinkRegionType = {
    val s = t._2 split "\t"
    (t._1, s(0), s(1).toLong, s(2).toLong, s(3).charAt(0), new Array[GValue](0))
  }

}
