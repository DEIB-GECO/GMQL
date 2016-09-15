package it.polimi.genomics.scidb.test.test

import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.compiler.Translator
import it.polimi.genomics.core.DataStructures.ExecutionParameters.BinningParameter
import it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.scidb.{GmqlSciAdapter, GmqlSciImplementation}
import it.polimi.genomics.scidb.test.FakeR2R

object TestMap01
{
  def main(args: Array[String]): Unit =
  {
    val query_scidb =
      "R = SELECT(NOT(leaveout==\"something\")) [RnaSeqParser]ANN;\n"+
        "E = SELECT(NOT(leaveout==\"something\")) [BedScoreParser]BED;\n"+
        "M = MAP(AVG(score)) R E;\n"+
        "MATERIALIZE M into OUTPUT_MAP;"

    val scidb_server = new GmqlServer(new GmqlSciAdapter())
    val scidb_translator = new Translator(scidb_server, "")

    scidb_translator.phase2(scidb_translator.phase1(query_scidb))
    val scidb_start: Long = System.currentTimeMillis
    scidb_server.run()
    val scidb_stop: Long = System.currentTimeMillis
    println(true, Some(scidb_stop - scidb_start))
  }
}
