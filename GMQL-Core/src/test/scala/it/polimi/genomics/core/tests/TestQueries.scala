package it.polimi.genomics.core.tests

import it.polimi.genomics.core.DataStructures.CoverParameters.{ALL, CoverFlag}
import it.polimi.genomics.core.DataStructures.{IRVariable, Instance}
import it.polimi.genomics.core.DataStructures.MetadataCondition.{META_OP, Predicate}
import it.polimi.genomics.core.DataStructures.RegionCondition.{REG_OP, StartCondition}

object TestQueries {

  private val metadataCondition = Predicate("attribute", META_OP.EQ, "ciao")
  private val regionCondition = StartCondition(REG_OP.GTE, 1000)


  /**
    * Query:
    *
    * V1 = SELECT(metadataCondition, regionCondition) dataset1
    * V2 = SELECT(metadataCondition, regionCondition) dataset2
    * V3 = MAP() V2 V1
    *
    * MATERIALIZE V1 INTO V1
    * MATERIALIZE V2 INTO V2
    * MATERIALIZE V3 INTO V3
    * */
  val query1: List[IRVariable] = {
    val v1 = TestUtils.getInitialIRVariable("dataset1", TestUtils.instances.head)
      .add_select_statement(None, None, Some(metadataCondition), Some(regionCondition), Some(TestUtils.instances.head))
    val v2 = TestUtils.getInitialIRVariable("dataset2", TestUtils.instances(1))
      .COVER(CoverFlag.COVER, new ALL{}, new ALL {}, List(TestUtils.getRegionsToRegion), None, Some(TestUtils.instances(1)))
    val v3 = TestUtils.doMAP(v2, v1, Some(TestUtils.instances(1)))
    List(
      TestUtils.materializeIRVariable(v1, "v1", Some(TestUtils.instances.head)),
      TestUtils.materializeIRVariable(v2, "v2", Some(TestUtils.instances.head)),
      TestUtils.materializeIRVariable(v3, "v3", Some(TestUtils.instances.head))
    )
  }

  /**
    * Query:
    *
    * V1 = SELECT(regionCondition) dataset1
    * MATERIALIZE V1 INTO V1
    * */
  val query2: List[IRVariable] = {
    val v1 = TestUtils.getInitialIRVariable("dataset1", TestUtils.instances(1))
      .add_select_statement(None, None, None, Some(regionCondition), Some(TestUtils.instances(1)))
    List(
      TestUtils.materializeIRVariable(v1, "v1", Some(TestUtils.instances(0))
    ))
  }

  /**
    * Query:
    *
    * V1 = SELECT(metadataCondition) dataset1
    * V11 = MERGE() V1
    *
    * V2 = SELECT(metadataCondition) dataset2
    * V21 = PROJECT(meta) V2
    *
    * V3 = SELECT(metadataCondition) dataset3
    *
    * V23 = JOIN() V2 V3
    * V123 = MAP() V1 V23
    *
    * MATERIALIZE V11 INTO V11
    * MATERIALIZE V123 INTO V123
    * */
  val query3: List[IRVariable] = {
    val v1 = TestUtils.getInitialIRVariable("dataset1", TestUtils.instances(0))
      .add_select_statement(None, None, Some(metadataCondition), None, Some(TestUtils.instances(0)))
      .MERGE(None, Some(TestUtils.instances(0)))
    val v2 = TestUtils.getInitialIRVariable("dataset2", TestUtils.instances(1))
      .add_select_statement(None, None, Some(metadataCondition), None, Some(TestUtils.instances(1)))
      .PROJECT(Some(List("meta")), None, false, None, None, None, Some(TestUtils.instances(1)))
    val v3 = TestUtils.getInitialIRVariable("dataset3", TestUtils.instances(2))
      .add_select_statement(None, None, Some(metadataCondition), None, Some(TestUtils.instances(2)))

    val v23 = TestUtils.doJOIN(v2, v3, Some(TestUtils.instances(1)))
    val v123 = TestUtils.doMAP(v1, v23, Some(TestUtils.instances(0)))

    List(
      TestUtils.materializeIRVariable(v1, "v1", Some(TestUtils.instances(0))),
      TestUtils.materializeIRVariable(v123, "v123", Some(TestUtils.instances(0)))
    )
  }

}
