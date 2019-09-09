package it.polimi.genomics.federated.tests

import it.polimi.genomics.core.DataStructures.CoverParameters.{ALL, CoverFlag, CoverParameterManager}
import it.polimi.genomics.core.DataStructures.JoinParametersRD.RegionBuilder
import it.polimi.genomics.core.DataStructures.{IRVariable, Instance, LOCAL_INSTANCE}
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


  /**Query 4:
    *
    * V1 = SELECT(metadataCondition) dataset1;
    * V2 = SELECT(metadataCondition, at: Loc2) V1;
    * MATERIALIZE V2 INTO V2;
    * */
  val query4 : List[IRVariable] = {
    val v1 = TestUtils.getInitialIRVariable("dataset1", TestUtils.instances(1))
      .add_select_statement(None, None, Some(metadataCondition), None, Some(TestUtils.instances(1)))
    val v2 = v1.add_select_statement(None, None, Some(metadataCondition), None, execute_location = Some(TestUtils.instances(2)))

    List(
      TestUtils.materializeIRVariable(v2, "v2", Some(TestUtils.instances(1)))
    )

  }

  /**
    * Publication query
    *
    * myExperiment = SELECT(at GMQL-U) UPLOAD;
    * myData = COVER(2, ANY; at GMQL-U) myExperiment;
    * genes = SELECT(annotation_type == "gene" AND provider == "RefSeq" at GMQL-A) HG19_BED_ANNOTATIONS;
    * onGenes = JOIN(distance < 0; output: right at GMQL-A) genes myData;
    * mutations = SELECT(type == "SNP" at GMQL-CINECA) ICGC_REPOSITORY;
    * geneMutationCount = MAP(at GMQL-CINECA) onGenes mutations;
    * MATERIALIZE geneMutationCount INTO result;
    * */

  val queryPub: List[IRVariable] = {
    val myExperiment = TestUtils.getInitialIRVariable("UPLOAD", Instance("GMQL-U"))
      .add_select_statement(None, None, None, None, Some(Instance("GMQL-U")))
    val myData = myExperiment.COVER(CoverFlag.COVER, CoverParameterManager.getCoverParam("N", Some(2)),
      CoverParameterManager.getCoverParam("ANY"), List.empty, None, Some(Instance("GMQL-U")))
    val genes = TestUtils.getInitialIRVariable("HG19_BED_ANNOTATIONS", Instance("GMQL-A"))
      .add_select_statement(None, None, None, None, Some(Instance("GMQL-A")))
    val onGenes = genes.JOIN(None, List(), RegionBuilder.RIGHT, myData, None, None, None, Some(Instance("GMQL-A")))
    val mutations = TestUtils.getInitialIRVariable("ICGC_REPOSITORY", Instance("GMQL-CINECA"))
      .add_select_statement(None, None, None, None, Some(Instance("GMQL-CINECA")))
    val geneMutationCount = onGenes.MAP(None, List.empty, mutations, None, None, None, Some(Instance("GMQL-CINECA")))

    List(
      TestUtils.materializeIRVariable(geneMutationCount, "", Some(LOCAL_INSTANCE))
    )
  }

  /**
   * Policy query
   *
   * X = SELECT() A@S1
   * X1 = COVER() X
   * X2 = MERGE() X1
   *
   * Y = SELECT() B@S2
   * XY = JOIN() X2 Y
   *
   * Z = SELECT() C@S3
   * XYZ = MAP() XY Z
   * XYZ1 = COVER() XYZ
   * MATERIALIZE XYZ1 @S4
   */

  val queryLocationPolicy: List[IRVariable] = {
    val X = TestUtils.getInitialIRVariable("A", Instance("S1"))
      .add_select_statement(None, None, None, None)
    val X1 = X.COVER(CoverFlag.COVER, CoverParameterManager.getCoverParam("N", Some(2)),
      CoverParameterManager.getCoverParam("ANY"), List.empty, None)
    val X2 = X1.MERGE(None)

    val Y = TestUtils.getInitialIRVariable("B", Instance("S2"))
      .add_select_statement(None, None, None, None)
    val XY = X2.JOIN(None, List(), RegionBuilder.RIGHT, Y, None, None, None)

    val Z = TestUtils.getInitialIRVariable("C", Instance("S3"))
      .add_select_statement(None, None, None, None)
    val XYZ = XY.MAP(None, List.empty, Z, None, None, None)
    val XYZ1 = XYZ.COVER(CoverFlag.COVER, CoverParameterManager.getCoverParam("N", Some(2)),
      CoverParameterManager.getCoverParam("ANY"), List.empty, None)
    List(
      TestUtils.materializeIRVariable(XYZ1, "", Some(LOCAL_INSTANCE))
    )
  }


  /**
   * Protected policy query
   *
   * X = SELECT() A@S1 (PROTECTED)
   * X1 = COVER() X
   * X2 = MERGE() X1
   *
   * Y = SELECT() B@S2
   * XY = JOIN() X2 Y
   *
   * Z = SELECT() C@S3
   * XYZ = MAP() XY Z
   * XYZ1 = COVER() XYZ
   * MATERIALIZE XYZ1 @S4
   */

  val queryProtectedPolicy: List[IRVariable] = {
    val X = TestUtils.getInitialIRVariable("A", Instance("S1"), protect = true)
      .add_select_statement(None, None, None, None)
    val X1 = X.COVER(CoverFlag.COVER, CoverParameterManager.getCoverParam("N", Some(2)),
      CoverParameterManager.getCoverParam("ANY"), List.empty, None)
    val X2 = X1.MERGE(None)

    val Y = TestUtils.getInitialIRVariable("B", Instance("S2"))
      .add_select_statement(None, None, None, None)
    val XY = X2.JOIN(None, List(), RegionBuilder.RIGHT, Y, None, None, None)

    val Z = TestUtils.getInitialIRVariable("C", Instance("S3"))
      .add_select_statement(None, None, None, None)
    val XYZ = XY.MAP(None, List.empty, Z, None, None, None)
    val XYZ1 = XYZ.COVER(CoverFlag.COVER, CoverParameterManager.getCoverParam("N", Some(2)),
      CoverParameterManager.getCoverParam("ANY"), List.empty, None)
    List(
      TestUtils.materializeIRVariable(XYZ1, "", Some(LOCAL_INSTANCE))
    )
  }
}
