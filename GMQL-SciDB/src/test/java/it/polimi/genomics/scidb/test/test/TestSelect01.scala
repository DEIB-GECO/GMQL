package it.polimi.genomics.scidb.test.test

import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.compiler.Translator
import it.polimi.genomics.core.DataStructures.ExecutionParameters.BinningParameter
import it.polimi.genomics.core.DataStructures.JoinParametersRD._
import it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition
import it.polimi.genomics.core.DataStructures.MetadataCondition._
import it.polimi.genomics.core.DataStructures.RegionCondition.{MetaAccessor, LeftEndCondition, StartCondition, ChrCondition}
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.scidb.utility.DebugUtils
import it.polimi.genomics.scidb.{GmqlSciAdapter, GmqlSciImplementation}
import it.polimi.genomics.core.DataStructures.MetadataCondition.META_OP._

/**
  * Created by Cattani Simone on 10/03/16.
  * Email: simone.cattani@mail.polimi.it
  *
  */
object TestSelect01
{
  def main(args: Array[String]): Unit =
  {
    val query_scidb = "S = SELECT(File_Number==\"0\") [BedScoreParser] BED_10000;\n MATERIALIZE S into OUTPUT_10000;"

    val scidb_server = new GmqlServer(new GmqlSciAdapter())
    val scidb_translator = new Translator(scidb_server, "")

    scidb_translator.phase2(scidb_translator.phase1(query_scidb))
    val scidb_start: Long = System.currentTimeMillis
    scidb_server.run()
    val scidb_stop: Long = System.currentTimeMillis
    println(true, Some(scidb_stop - scidb_start))

  }
}
