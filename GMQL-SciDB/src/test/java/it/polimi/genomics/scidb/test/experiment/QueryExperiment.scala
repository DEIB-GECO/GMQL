package it.polimi.genomics.scidb.test.experiment

import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.compiler.Translator
import it.polimi.genomics.scidb.GmqlSciAdapter

/**
  * Created by Cattani Simone on 02/06/16.
  * Email: simone.cattani@mail.polimi.it
  *
  */
object QueryExperiment
{
  def main(args: Array[String]): Unit =
  {
    var res : List[Long] = List()
    for(i <- List("H", "K", "0K")){
      for(j <- List("1", "2", "5")){

        val query_scidb = "" +
          "R = SELECT(NOT(leaveout==\"something\")) [RnaSeqParser]EXP_01_REF3K;\n"+
          "E = SELECT(NOT(leaveout==\"something\"))  [BedScoreParser]EXP_01_EXP1H_"+j+i+";\n" +
          "M = MAP(BAG(score)) R E;\n" +
          "MATERIALIZE M into TEST_EXP3K_"+j+i+";"

        val scidb_server = new GmqlServer(new GmqlSciAdapter())
        val scidb_translator = new Translator(scidb_server, "")

        scidb_translator.phase2(scidb_translator.phase1(query_scidb))

        val scidb_start: Long = System.currentTimeMillis
        scidb_server.run()
        val scidb_stop: Long = System.currentTimeMillis

        println("Time "+j+i+": "+(scidb_stop - scidb_start))
        res = res ::: List((scidb_stop - scidb_start))
      }
    }

    println("["+res.mkString(",")+"]")
  }
}
