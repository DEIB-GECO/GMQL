package it.polimi.genomics.core.tests

import it.polimi.genomics.core.DAG.DAG
import it.polimi.genomics.core.DataStructures.CoverParameters.{ALL, CoverFlag}
import it.polimi.genomics.core.DataStructures.Instance
import it.polimi.genomics.core.DataStructures.JoinParametersRD.JoinQuadruple
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
  val query1: DAG = {
    val v1 = TestUtils.getInitialIRVariable("dataset1", TestUtils.instances.head)
      .SELECT(metadataCondition, regionCondition)
    val v2 = TestUtils.getInitialIRVariable("dataset2", TestUtils.instances(1))
      .COVER(CoverFlag.COVER, new ALL{}, new ALL {}, List(TestUtils.getRegionsToRegion), None)
    val v3 = TestUtils.doMAP(v2, v1)
    val dag = new DAG(List(
      TestUtils.materializeIRVariable(v1, "v1"),
      TestUtils.materializeIRVariable(v2, "v2"),
      TestUtils.materializeIRVariable(v3, "v3")
    ))
    dag
  }

  /**
    * Query:
    *
    * V1 = SELECT(regionCondition) dataset1
    * MATERIALIZE V1 INTO V1
    * */
  val query2: DAG = {
    val v1 = TestUtils.getInitialIRVariable("dataset1", TestUtils.instances.head)
      .SELECT(regionCondition)
    new DAG(List(
      TestUtils.materializeIRVariable(v1, "v1")
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
  val query3: DAG = {
    val v1 = TestUtils.getInitialIRVariable("dataset1", Instance("L1"))
      .SELECT(metadataCondition).MERGE(None)
    val v2 = TestUtils.getInitialIRVariable("dataset2", Instance("L2"))
      .SELECT(metadataCondition).PROJECT(Some(List("meta")))
    val v3 = TestUtils.getInitialIRVariable("dataset3", Instance("L3"))
      .SELECT(metadataCondition)

    val v23 = TestUtils.doJOIN(v2, v3)
    val v123 = TestUtils.doMAP(v1, v23)

    new DAG(List(
      TestUtils.materializeIRVariable(v1, "v1"),
      TestUtils.materializeIRVariable(v123, "v123")
    ))
  }

}
