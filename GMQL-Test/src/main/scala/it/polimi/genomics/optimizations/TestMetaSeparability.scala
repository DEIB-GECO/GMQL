package it.polimi.genomics.optimizations

import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.core.DataStructures.CoverParameters.{ALL, CoverFlag}
import it.polimi.genomics.core.DataStructures.MetadataCondition.{META_OP, NOT, Predicate}
import it.polimi.genomics.core.DataStructures.RegionCondition
import it.polimi.genomics.core.DataStructures.RegionCondition.REG_OP
import it.polimi.genomics.pythonapi.StubExecutor
import it.polimi.genomics.spark.implementation.loaders.BedParser

/**
  * Created by Luca Nanni on 23/11/17.
  * Email: luca.nanni@mail.polimi.it
  */
object TestMetaSeparability {

  val gmqlServer = new GmqlServer(new StubExecutor())

  def main(args: Array[String]): Unit = {
    // building the dag
    var iRVariable = gmqlServer.READ("FOO_DATASET").USING(BedParser)
    iRVariable = iRVariable.SELECT(NOT(Predicate("meta", META_OP.EQ, "boh")),
      RegionCondition.NOT(RegionCondition.StartCondition(REG_OP.GT,3000)))
    iRVariable = iRVariable.COVER(CoverFlag.COVER, new ALL{}, new ALL{}, List(), None)
    gmqlServer setOutputPath "outputPath" MATERIALIZE iRVariable
    gmqlServer.optimise(gmqlServer.materializationList.toList)
  }
}
