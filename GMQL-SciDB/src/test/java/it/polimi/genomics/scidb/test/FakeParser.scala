package it.polimi.genomics.scidb.test

import it.polimi.genomics.core.{DataTypes, GMQLLoader}

/**
  * Created by pietro on 05/04/16.
  */
class FakeParser extends GMQLLoader[(Long,String), DataTypes.FlinkRegionType, (Long,String), DataTypes.FlinkMetaType]  {
  override def meta_parser(t : (Long, String)) = null
  override def region_parser(t : (Long, String)) = null
}
