package it.polimi.genomics.scidb.test.experiment

import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.compiler.Translator
import it.polimi.genomics.scidb.GmqlSciAdapter

/**
  * Created by Cattani Simone on 02/06/16.
  * Email: simone.cattani@mail.polimi.it
  *
  */
object SingleExperiment
{
  def main(args: Array[String]): Unit =
  {
    val name = "1H"

    val query_scidb = "" +
      "R = SELECT(NOT(leaveout==\"something\")) [RnaSeqParser]EXP_01_REF3K;\n"+
      "E = SELECT(NOT(leaveout==\"something\"))  [BedScoreParser]EXP_01_EXP1H_"+name+";\n" +
      "M = MAP(BAG(score)) R E;\n" +
      "MATERIALIZE M into TEST_EXP3K_"+name+";"

    val scidb_server = new GmqlServer(new GmqlSciAdapter())
    val scidb_translator = new Translator(scidb_server, "")

    scidb_translator.phase2(scidb_translator.phase1(query_scidb))

    val scidb_start: Long = System.currentTimeMillis
    scidb_server.run()
    val scidb_stop: Long = System.currentTimeMillis

    println("Time: "+(scidb_stop - scidb_start))
  }
}
