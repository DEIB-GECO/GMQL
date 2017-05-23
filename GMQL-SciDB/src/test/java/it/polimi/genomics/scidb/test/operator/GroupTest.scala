package it.polimi.genomics.scidb.test.operator

import it.polimi.genomics.core.DataStructures.ExecutionParameters.BinningParameter
import it.polimi.genomics.core.DataStructures.GroupRDParameters.FIELD
import it.polimi.genomics.core.DataStructures.MetaGroupByCondition.MetaGroupByCondition
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.scidb.GmqlSciImplementation
import it.polimi.genomics.scidb.test.{FakeR2R, FakeR2M}

/**
  * Created by Cattani Simone on 15/04/16.
  * Email: simone.cattani@mail.polimi.it
  *
  */
object GroupTest
{
  def main(args: Array[String]): Unit =
  {
    println("\n-----------------------------------------------")
    println("-- Join TEST ----------------------------------")
    println("-----------------------------------------------")
    import scala.collection.JavaConverters._
    val SOURCE = IRDataSet("DEV_GROUPRD_REF", List(
      ("val1",ParsingType.INTEGER),
      ("val2",ParsingType.STRING)
    ).asJava)

    val RES = IRDataSet("DEV_GROUPRD_RES", List(
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

    var maxVal1R = new FakeR2R
    maxVal1R.function_identifier = "max"
    maxVal1R.input_index = 0
    maxVal1R.output_name = Some("max_val1")

    var minVal1R = new FakeR2R
    minVal1R.function_identifier = "min"
    minVal1R.input_index = 0
    minVal1R.output_name = Some("min_val1")

    val countVal1R = new FakeR2R
    countVal1R.function_identifier = "count"
    countVal1R.input_index = 0
    countVal1R.output_name = Some("count")

    val GroupMD = IRGroupMD(MetaGroupByCondition(List("bert_value1")), List(minVal1, maxVal1, countVal1), "group", ReadSourceMD, ReadSourceRD)
    val GroupRD = IRGroupRD(Some(List(FIELD(1))), Some(List(maxVal1R, minVal1R, countVal1R)), ReadSourceRD)

    val StoreAncMD = IRStoreMD("", GroupMD, RES)
    val StoreAncRD = IRStoreRD("", GroupRD,GroupMD,List(), RES)

    implicit val binS = BinningParameter(None)
    var variable = IRVariable(StoreAncMD,StoreAncRD)

    // ------------------------------------------------------------
    // ------------------------------------------------------------

    val implementation = new GmqlSciImplementation

    implementation.addDAG(variable)
    implementation.go()
  }
}
