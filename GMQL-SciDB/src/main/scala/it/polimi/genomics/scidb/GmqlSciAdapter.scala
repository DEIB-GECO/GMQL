package it.polimi.genomics.scidb

import it.polimi.genomics.GMQLServer.Implementation
import it.polimi.genomics.core.DataStructures.IRDataSet
import it.polimi.genomics.core.{DataTypes, GMQLLoader, GMQLLoaderBase}
import it.polimi.genomics.scidb.operators.GDS

/**
  * Created by Cattani Simone on 06/04/16.
  * Email: simone.cattani@mail.polimi.it
  *
  */
class GmqlSciAdapter
  extends Implementation
{
  val implementation = new GmqlSciImplementation

  override def getDataset(identifier : String) : Option[IRDataSet] = implementation.getDataset(identifier)

  /** Starts the execution */
  override def go(): Unit =
  {
    for (variable <- to_be_materialized) {
      implementation.addDAG(variable)
    }

    implementation.go()
  }

  override def stop():Unit ={

  }


  class FakeParser extends GMQLLoader[(Long,String), DataTypes.FlinkRegionType, (Long,String), DataTypes.FlinkMetaType]  {
    override def meta_parser(t : (Long, String)) = null
    override def region_parser(t : (Long, String)) = null
  }


  /** given the name of a parser, returns it. It must at least provide a parser for the "default" name */
  override def getParser(name: String, dataset: String): GMQLLoaderBase = new FakeParser
}