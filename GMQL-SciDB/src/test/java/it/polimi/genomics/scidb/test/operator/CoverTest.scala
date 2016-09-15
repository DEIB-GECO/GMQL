package it.polimi.genomics.scidb.test.operator

import it.polimi.genomics.core.DataStructures.CoverParameters.{N, ANY, CoverFlag}
import it.polimi.genomics.core.DataStructures.ExecutionParameters.BinningParameter
import it.polimi.genomics.core.DataStructures.MetaGroupByCondition.MetaGroupByCondition
import it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.scidb.GmqlSciImplementation
import it.polimi.genomics.scidb.test.FakeR2R

/**
  * Created by Cattani Simone on 13/04/16.
  * Email: simone.cattani@mail.polimi.it
  *
  */
object CoverTest
{
  def main(args: Array[String]): Unit =
  {
    println("\n-----------------------------------------------")
    println("-- Map TEST -----------------------------------")
    println("-----------------------------------------------")
    import scala.collection.JavaConverters._
    val ANC = IRDataSet("DEV_COVER_REF", List(
      ("val1",ParsingType.INTEGER),
      ("val2",ParsingType.STRING)
    ).asJava)

    val RES = IRDataSet("DEV_COVER_RES", List(
      ("val1",ParsingType.INTEGER),
      ("val2",ParsingType.STRING)
    ).asJava)

    val ReadAncMD = IRReadMD(List(), new FakeParser, ANC)
    val ReadAncRD = IRReadRD(List(), new FakeParser, ANC)

    var maxVal1 = new FakeR2R
    maxVal1.function_identifier = "min"
    maxVal1.input_index = 0
    maxVal1.output_name = Some("max_val1")

    var countVal1 = new FakeR2R
    countVal1.function_identifier = "count"
    countVal1.input_index = 0
    countVal1.output_name = Some("counted")

    val MetaGroup = IRGroupBy(MetaGroupByCondition(List("bert_value1")), ReadAncMD)
    val CollapseMD = IRCollapseMD(Some(MetaGroup), ReadAncMD)
    val CoverMD = IRRegionCover(CoverFlag.FLAT, N(2), ANY(), List(countVal1), Some(MetaGroup), ReadAncRD)

    val StoreAncMD = IRStoreMD("", CollapseMD, RES)
    val StoreAncRD = IRStoreRD("", CoverMD, RES)

    implicit val binS = BinningParameter(None)
    var variable = IRVariable(StoreAncMD,StoreAncRD)

    // ------------------------------------------------------------
    // ------------------------------------------------------------

    val implementation = new GmqlSciImplementation

    implementation.addDAG(variable)
    implementation.go()

  }
}
