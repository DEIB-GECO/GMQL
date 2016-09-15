package it.polimi.genomics.scidb.test.operator

import it.polimi.genomics.core.DataStructures.ExecutionParameters.BinningParameter
import it.polimi.genomics.core.DataStructures.JoinParametersRD._
import it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.scidb.GmqlSciImplementation
import it.polimi.genomics.scidb.test.FakeR2R

object MapTest
{
  def main(args: Array[String]): Unit =
  {
    println("\n-----------------------------------------------")
    println("-- Map TEST -----------------------------------")
    println("-----------------------------------------------")
    import scala.collection.JavaConverters._
    val ANC = IRDataSet("DEV_MAP_REF", List(
      ("val1",ParsingType.INTEGER),
      ("val2",ParsingType.STRING)
    ).asJava)

    val EXP = IRDataSet("DEV_MAP_EXP", List(
      ("val1",ParsingType.INTEGER),
      ("val2",ParsingType.STRING)
    ).asJava)

    val RES = IRDataSet("DEV_MAP_RES", List(
      ("val1",ParsingType.INTEGER),
      ("val2",ParsingType.STRING)
    ).asJava)

    val ReadAncMD = IRReadMD(List(), new FakeParser, ANC)
    val ReadAncRD = IRReadRD(List(), new FakeParser, ANC)
    val ReadExpMD = IRReadMD(List(), new FakeParser, EXP)
    val ReadExpRD = IRReadRD(List(), new FakeParser, EXP)

    val MetaJoinMJD = IRJoinBy(MetaJoinCondition(List("bert_value1")), ReadAncMD, ReadExpMD)
    val CombineMD = IRCombineMD(SomeMetaJoinOperator(MetaJoinMJD), ReadAncMD, ReadExpMD)

    var maxVal1 = new FakeR2R
    maxVal1.function_identifier = "MAX"
    maxVal1.input_index = 0
    maxVal1.output_name = Some("max_val1")

    var minVal1 = new FakeR2R
    minVal1.function_identifier = "MIN"
    minVal1.input_index = 0
    minVal1.output_name = Some("min_val1")

    var countVal1 = new FakeR2R
    countVal1.function_identifier = "COUNT"
    countVal1.input_index = 0
    countVal1.output_name = Some("count")

    val GenometriMap = IRGenometricMap(SomeMetaJoinOperator(MetaJoinMJD), List(maxVal1, minVal1, countVal1), ReadAncRD, ReadExpRD)

    val StoreAncMD = IRStoreMD("", CombineMD, RES)
    val StoreAncRD = IRStoreRD("", GenometriMap, RES)

    implicit val binS = BinningParameter(None)
    var variable = IRVariable(StoreAncMD,StoreAncRD)

    // ------------------------------------------------------------
    // ------------------------------------------------------------

    val implementation = new GmqlSciImplementation

    implementation.addDAG(variable)
    implementation.go()

  }
}
