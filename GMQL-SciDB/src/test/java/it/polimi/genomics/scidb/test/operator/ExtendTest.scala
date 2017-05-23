package it.polimi.genomics.scidb.test.operator

import it.polimi.genomics.core.DataStructures.ExecutionParameters.BinningParameter
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.scidb.GmqlSciImplementation
import it.polimi.genomics.scidb.test.{FakeR2M, FakeR2R}

/**
  * Created by Cattani Simone on 14/04/16.
  * Email: simone.cattani@mail.polimi.it
  *
  */
object ExtendTest
{
  def main(args: Array[String]): Unit =
  {
    println("\n-----------------------------------------------")
    println("-- Join TEST ----------------------------------")
    println("-----------------------------------------------")
    import scala.collection.JavaConverters._
    val SOURCE = IRDataSet("DEV_MAP_REF", List(
      ("val1",ParsingType.INTEGER),
      ("val2",ParsingType.STRING)
    ).asJava)

    val RES = IRDataSet("DEV_SELECT_RES", List(
      ("val1",ParsingType.INTEGER),
      ("val2",ParsingType.STRING)
    ).asJava)

    val ReadSourceMD = IRReadMD(List(), new FakeParser, SOURCE)
    val ReadSourceRD = IRReadRD(List(), new FakeParser, SOURCE)

    var minVal1 = new FakeR2M
    minVal1.function_identifier = "min"
    minVal1.input_index = 0
    minVal1.output_attribute_name = "val1_min"

    var maxVal1 = new FakeR2M
    maxVal1.function_identifier = "max"
    maxVal1.input_index = 0
    maxVal1.output_attribute_name = "val1_max"

    var countVal1 = new FakeR2M
    countVal1.function_identifier = "count"
    countVal1.input_index = 0
    countVal1.output_attribute_name = "count"

    val AggregateMD = IRAggregateRD(List(minVal1, maxVal1, countVal1), ReadSourceRD)
    val ExtendMD = IRUnionAggMD(ReadSourceMD, AggregateMD)

    val StoreAncMD = IRStoreMD("", ExtendMD, RES)
    val StoreAncRD = IRStoreRD("", ReadSourceRD,ExtendMD,List(), RES)

    implicit val binS = BinningParameter(None)
    var variable = IRVariable(StoreAncMD,StoreAncRD)

    // ------------------------------------------------------------
    // ------------------------------------------------------------

    val implementation = new GmqlSciImplementation

    implementation.addDAG(variable)
    implementation.go()
  }
}
