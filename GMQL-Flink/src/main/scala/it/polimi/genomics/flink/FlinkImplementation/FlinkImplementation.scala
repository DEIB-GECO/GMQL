package it.polimi.genomics.flink.FlinkImplementation

import java.io.File

import it.polimi.genomics.GMQLServer.Implementation
import it.polimi.genomics.core.DataStructures.GroupMDParameters.Direction._
import it.polimi.genomics.core.DataStructures.GroupMDParameters.TopParameter
import it.polimi.genomics.core.DataStructures.MetaAggregate.MetaExtension
import it.polimi.genomics.core.DataStructures.MetaGroupByCondition.MetaGroupByCondition
import it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition
import it.polimi.genomics.core.DataStructures.MetadataCondition.MetadataCondition
import it.polimi.genomics.core.DataStructures.RegionAggregate.{RegionExtension, RegionsToMeta}
import it.polimi.genomics.core.DataStructures.RegionCondition.RegionCondition
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.core.{BinSize, GMQLLoader, GMQLLoaderBase, GMQLSchemaFormat}
import it.polimi.genomics.flink.FlinkImplementation.operator.meta._
import it.polimi.genomics.flink.FlinkImplementation.operator.metaGroup.MetaGroupMGD
import it.polimi.genomics.flink.FlinkImplementation.operator.metaJoin.MetaJoinMJD3
import it.polimi.genomics.flink.FlinkImplementation.operator.region._
import it.polimi.genomics.flink.FlinkImplementation.reader.parser._
import it.polimi.genomics.flink.FlinkImplementation.writer.{DefaultRegionWriter, MetaWriter, RegionWriter}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.slf4j.LoggerFactory

/** Translate the variable dags into a Flink workflow and executed it*/
class FlinkImplementation(var binSize : BinSize = BinSize(),
                          val maxBinDistance : Int = 1000000,
                          testingIOFormats : Boolean = false,
                          metaFirst : Boolean = false, outputFormat:GMQLSchemaFormat.Value = GMQLSchemaFormat.TAB) extends Implementation with java.io.Serializable{

  val overwrite_option = WriteMode.OVERWRITE  // (or NO_OVERWRITE)

  final val logger = LoggerFactory.getLogger(this.getClass)

  override def collect(iRVariable: IRVariable): Any = ???

  override def take(iRVariable: IRVariable, n: Int): Any = ???

  def getParser(name : String,dataset:String) : GMQLLoaderBase = {
    name.toLowerCase() match {
      case "bedscoreparser" => BedScoreParser
      case "rnaseqparser" => RnaSeqParser
      case "basicparser" => BasicParser
      case "narrow" => NarrowPeakParser
      case "narrowpeak" => NarrowPeakParser
      case "narrowpeakparser" => NarrowPeakParser
      case "broad" => BroadPeakParser
      case "broadpeak" => BroadPeakParser
      case "broadpeakparser" => BroadPeakParser
      case "broadpeaksparser" => BroadPeakParser
      case "annparser" => ANNParser
      case "broadgenesparser" => BroadGenesParser
      case "default" => BedScoreParser
      case "customparser" => RepositoryParser(dataset)
      case _ => {logger.warn("unable to find " + name + " parser, try the default one"); getParser("default",dataset)}
    }
  }


  def go(): Unit = {
    logger.info(" -- Executing GMQL on Apache Flink -- ")
    logger.debug("Variable to be materialized:" + to_be_materialized.mkString("\n") + "\n")
//    logger.debug("Possible ordered execution: - experimental - \n\t" + GraphEnumerator(to_be_materialized.toList).mkString("\n\t") + "\n")
    dummy_implementation()
  }


def stop(): Unit ={

}

  def computeMetaFirst() : Seq[FlinkMetaType] = {
    logger.debug(" -- Executing MetaFirst on Apache Flink -- ")
    metaFirstImplementation()
  }

  /** Easiest implementation: no optimization is done. */
  def dummy_implementation(): Unit = {

    // TODO change to parallel execution
    val env = ExecutionEnvironment.getExecutionEnvironment
//    val env = ExecutionEnvironment.createLocalEnvironment(1)

    env.getConfig.disableSysoutLogging()

//    env.getConfig.setExecutionMode(ExecutionMode.BATCH);
    // TODO benchmarking
    val ms = System.currentTimeMillis();

//    try {
      for (variable <- to_be_materialized) {

//        val hdfsFS = FSR_Utilities.gethdfsConfiguration().get("fs.defaultFS")

        val metaOutputPath : String =
          if(!variable.metaDag.asInstanceOf[IRStoreMD].path.toString.toLowerCase().startsWith("job")){
            variable.metaDag.asInstanceOf[IRStoreMD].path.toString + "/meta/"
          } else {
//            if (General_Utilities().MODE == General_Utilities().HDFS) {
              /*hdfsFS + General_Utilities().getHDFSRegionDir() +*/ variable.regionDag.asInstanceOf[IRStoreRD].path.toString + "/meta/"
//            } else {
//              General_Utilities().getRegionDir() + variable.regionDag.asInstanceOf[IRStoreRD].path.toString + "/meta/"
            }
//          }

        val regionOutputPath : String =
        if(!variable.regionDag.asInstanceOf[IRStoreRD].path.toString.toLowerCase().startsWith("job")){
          variable.regionDag.asInstanceOf[IRStoreRD].path.toString + "/exp/"
        } else {
//          if (General_Utilities().MODE == General_Utilities().HDFS) {
//            hdfsFS + General_Utilities().getHDFSRegionDir() + variable.regionDag.asInstanceOf[IRStoreRD].path.toString + "/exp/"
//          } else {
           /* General_Utilities().getRegionDir() +*/ variable.regionDag.asInstanceOf[IRStoreRD].path.toString + "/exp/"
//          }
        }

        val schemaOutputPath : String =
          if(!variable.regionDag.asInstanceOf[IRStoreRD].path.toString.toLowerCase().startsWith("job") && !variable.regionDag.asInstanceOf[IRStoreRD].path.toString.toLowerCase().startsWith("hdfs")){
            new File(variable.regionDag.asInstanceOf[IRStoreRD].path).toString + "/exp/test.schema"
          } else {
//            if(variable.regionDag.asInstanceOf[IRStoreRD].path.toString.toLowerCase().startsWith("hdfs"))
              (new org.apache.hadoop.fs.Path(variable.regionDag.asInstanceOf[IRStoreRD].path)).toString + "/exp/test.schema"
//            else
//              General_Utilities().getSchemaDir() + (new File(variable.regionDag.asInstanceOf[IRStoreRD].path)).toString + ".schema"
          }

        logger.info("Meta path = " + metaOutputPath)
        logger.info("Region path = " + regionOutputPath)
        logger.info("Schema path = " + schemaOutputPath)






        // Compute and store meta

        //Meta come first
        // implement_md(variable.metaDag, env).print
        val metaDS = implement_md(variable.metaDag, env)
        // if testingOutputFormat file are created according to parallelism level, each region has the sample id output
        // if false one file is created for each sample
          if(testingIOFormats){
            // write with default output writer
            metaDS
              .writeAsCsv(metaOutputPath, writeMode = overwrite_option)
          } else {
//             partition data according to sample id, sort them so to group by sample id, write with custom output writer
//            println("metaout: "+metaOutputPath)
            metaDS
              .partitionByHash(0)
              .sortPartition(0, Order.ASCENDING)
              .write(new MetaWriter(), metaOutputPath, writeMode = overwrite_option)
          }

      //  Utilities.getInstance().copyfiletoLocal("hdfs://localhost:9000/user/abdulrahman/regions/job_test_abdulrahman_20151028_113128_outds/exp/","/User/abdulrahman/test/")
        // Compute and store regions

        // Region Second
        // implement_rd(variable.regionDag, env).print
        val regionDS = implement_rd(variable.regionDag, env)
//        println("meta count : "+metaDS.count())
//       val metaids = metaDS.map(x=>(x._1)).distinct(t=>t).collect().sortBy(t=>t)//.foreach(x=> logger.info("Meta out ids: "+x))
//        val regionsids = regionDS.map((_._1)).distinct(t=>t).collect().sortBy(t=>t)//.foreach(x=> logger.info("regions out ids: "+x))
//        metaids.zip(regionsids).foreach(x=> logger.info(" ids: "+x))
//        logger.info("regions Size : "+ regionDS.count())
        // if testingOutputFormat file are created according to parallelism level, each region has the sample id output
        // if false one file is created for each sample

          if(testingIOFormats){
            // write with default output writer
            regionDS
              .write(new DefaultRegionWriter(), regionOutputPath, writeMode = overwrite_option)
          } else {
            // partition data according to sample id, sort them so to group by sample id, write with custom output writer
            regionDS
              .partitionByHash(0)
              .sortPartition(0, Order.ASCENDING)
              .write(new RegionWriter(), regionOutputPath, writeMode = overwrite_option)
          }




        // Compute and store schema

        // Schema third
        val schemaText = generateSchemaText(variable.schema)
        println ("output schema: "+schemaOutputPath)
        env.fromElements[String](schemaText).writeAsText(schemaOutputPath, writeMode = overwrite_option)

      }
//    } catch {
//      case e : SelectFormatException => {
//        logger.error(e.getMessage)
//      }
//      case e : Throwable => {
//        logger.error(e.toString)
//      }
//    }
    env.execute("GMQL_on_Flink")

    // TODO benchmarking
    logger.info((System.currentTimeMillis() - ms) + "ms total in Flink")
  }



  def metaFirstImplementation() : Seq[FlinkMetaType] = {

    // TODO change to parallel execution
    //val env = ExecutionEnvironment.getExecutionEnvironment
    val env = ExecutionEnvironment.createLocalEnvironment(8)

    // TODO benchmarking
    val ms = System.currentTimeMillis();

    var res : Seq[FlinkMetaType] =
//      try {
//        val a : Seq[FlinkMetaType] =
          to_be_materialized
            .flatMap((variable : IRVariable) => {
              implement_md(variable.metaDag, env).filter((v) => v._2.equals("GMQL_metafirst_id")).collect()
            })
//        a
//      } catch {
//        case e : SelectFormatException => {
//          logger.error(e.getMessage)
//          null
//        }
//        case e : Throwable => {
//          logger.error(e.toString)
//          null
//        }
//      }

    //env.execute("GMQL_on_Flink_metaFirst")

    // TODO benchmarking
    logger.info((System.currentTimeMillis() - ms) + "ms total in Flink - meta first")

    res
  }



  // Meta Data methods

  @throws[SelectFormatException]
  def implement_md(mo: MetaOperator, env : ExecutionEnvironment) : DataSet[FlinkMetaType] = {
    if(mo.intermediateResult.isDefined){
      logger.info(mo + " defined - recycling partial result")
      mo.intermediateResult.get.asInstanceOf[DataSet[FlinkMetaType]]
    } else {
      logger.info(mo + " not defined - executing it")
      val res =
        mo match {
          case IRStoreMD(path: String, value: MetaOperator,_) => StoreMD(this, path, value, env)
          case IRReadMD(paths: List[String], loader: GMQLLoader[Any, Any, Any, Any],_) => ReadMD(paths, loader, metaFirst, env, testingIOFormats = testingIOFormats)
          case IRSelectMD(metaCondition: MetadataCondition, inputDataset: MetaOperator) => SelectMD(this, metaCondition, inputDataset, false, env)
          case IRPurgeMD(regionDataset: RegionOperator, inputDataset: MetaOperator) => PurgeMD(this, regionDataset, inputDataset, env)
          case IRSemiJoin(externalMeta: MetaOperator, joinCondition: MetaJoinCondition, inputDataset: MetaOperator) => SemiJoinMD(this, externalMeta, joinCondition, inputDataset, env)
          case IRProjectMD(projectedAttributes: Option[List[String]], metaAggregator: Option[MetaExtension], inputDataset: MetaOperator) => ProjectMD(this, projectedAttributes, metaAggregator, inputDataset, env)
          case IRUnionMD(leftDataset: MetaOperator, rightDataset: MetaOperator, leftName : String, rightName : String) => UnionMD(this, leftDataset, rightDataset, leftName, rightName, env)
          case IRUnionAggMD(leftDataset: MetaOperator, rightDataset: MetaOperator, leftName : String, rightName : String) => UnionAggMD(this, leftDataset, rightDataset, leftName, rightName, env)
          case IRAggregateRD(aggregator: List[RegionsToMeta], inputDataset: RegionOperator) => AggregateRD(this, aggregator, inputDataset, env)
          case IRCombineMD(grouping: OptionalMetaJoinOperator, leftDataset: MetaOperator, rightDataset: MetaOperator, leftName : String, rightName : String) => CombineMD(this, grouping, leftDataset, rightDataset, leftName, rightName, env)
          case IRMergeMD(dataset: MetaOperator, groups: Option[MetaGroupOperator]) => MergeMD(this, dataset, groups, env)
          case IROrderMD(ordering: List[(String, Direction)], newAttribute: String, topParameter: TopParameter, inputDataset: MetaOperator) => OrderMD(this, ordering, newAttribute, topParameter, inputDataset, env)
          case IRGroupMD(keys: MetaGroupByCondition, aggregates: List[RegionsToMeta], groupName: String, inputDataset: MetaOperator, region_dataset: RegionOperator) => GroupMD(this, keys, aggregates, groupName, inputDataset, region_dataset, env)
          case IRCollapseMD(grouping : Option[MetaGroupOperator], inputDataset : MetaOperator) => CollapseMD(this, grouping, inputDataset, env)
        }
      mo.intermediateResult = Some(res)
      res
    }
  }

  // Region Data methods

  @throws[SelectFormatException]
  def implement_rd(ro : RegionOperator, env : ExecutionEnvironment) : DataSet[FlinkRegionType] = {
    if(ro.intermediateResult.isDefined){
      logger.info(ro + " defined - recycling partial result")
      ro.intermediateResult.get.asInstanceOf[DataSet[FlinkRegionType]]
    } else {
      logger.info(ro + " not defined - executing it")
      val res =
        ro match {
          case IRStoreRD(path: String, value: RegionOperator,meta:MetaOperator,schema,_) => StoreRD(this, path, value, env)
          case IRReadRD(paths: List[String], loader: GMQLLoader[Any, Any, Any, Any],_) => ReadRD(paths, loader, metaFirst, env, testingIOFormats = testingIOFormats)
          case IRSelectRD(regionCondition: Option[RegionCondition], filteredMeta: Option[MetaOperator], inputDataset: RegionOperator) => SelectRD(this, regionCondition, filteredMeta, inputDataset, metaFirst, env)
          case IRPurgeRD(metaDataset: MetaOperator, inputDataset: RegionOperator) => PurgeRD(this, metaDataset, inputDataset, env)
          case IRProjectRD(projectedValues: Option[List[Int]], tupleAggregator: Option[List[RegionExtension]], inputDataset: RegionOperator, inputMeta:MetaOperator) => ProjectRD(this, projectedValues, tupleAggregator, inputDataset, env)
          case IRUnionRD(schemaReformatting: List[Int], leftDataset: RegionOperator, rightDataset: RegionOperator) => UnionRD(this, schemaReformatting, leftDataset, rightDataset, env)
          case IRMergeRD(dataset: RegionOperator, groups: Option[MetaGroupOperator]) => MergeRD(this, dataset, groups, env)
          case IRGroupRD(groupingParameters: Option[List[GroupRDParameters.GroupingParameter]], aggregates: Option[List[RegionAggregate.RegionsToRegion]], regionDataset: RegionOperator) => GroupRD(this, groupingParameters, aggregates, regionDataset, env)
          case IROrderRD(ordering: List[(Int, Direction)], topPar: TopParameter, inputDataset: RegionOperator) => OrderRD(this, ordering: List[(Int, Direction)], topPar, inputDataset, env)
          case node : IRGenometricMap => GenometricMap4(this, node.grouping, node.aggregates, node.reference, node.samples, node.binSize.getOrElse(binSize.Map), env)
          case node : IRRegionCover=> GenometricCover2(this, node.cover_flag, node.min, node.max, node.aggregates, node.groups, node.input_dataset, node.binSize.getOrElse(binSize.Cover), env)
          case node : IRGenometricJoin => GenometricJoin4(this, node.metajoin_condition, node.join_condition, node.region_builder, node.left_dataset, node.right_dataset, env, node.binSize.getOrElse(binSize.Join), /*node.binSize.getOrElse(defaultBinSize) **/ maxBinDistance)
          case node : IRDifferenceRD => GenometricDifference2(this, node.meta_join, node.left_dataset, node.right_dataset, node.binSize.getOrElse(binSize.Map), env)

        }
      ro.intermediateResult = Some(res)
      res
    }
  }


  @throws[SelectFormatException]
  def implement_mjd3(mjo : OptionalMetaJoinOperator, env : ExecutionEnvironment) : DataSet[FlinkMetaJoinType] = {
//    if(mjo.intermediateResult.isDefined){
//      logger.info(mjo + " defined - recycling partial result")
//      mjo.intermediateResult.get.asInstanceOf[DataSet[FlinkMetaJoinType]]
//    } else {
//      logger.info(mjo + " not defined - executing it")
//      val res =
//        mjo match {
//          case IRJoinBy(condition: MetaJoinCondition, leftDataset: MetaOperator, rightDataset: MetaOperator) => MetaJoinMJD3(this, condition, leftDataset, rightDataset, env)
//        }
//      mjo.intermediateResult = Some(res)
//      res
//    }
    if(mjo.isInstanceOf[NoMetaJoinOperator]){
      if(mjo.asInstanceOf[NoMetaJoinOperator].operator.intermediateResult.isDefined) {
        logger.info(mjo + " NoneMJoin defined - recycling partial result")
        mjo.asInstanceOf[NoMetaJoinOperator].operator.intermediateResult.get.asInstanceOf[DataSet[FlinkMetaJoinType]]
      }
      else {
        logger.info(mjo + " not defined - executing it")
        val res =
          mjo.asInstanceOf[NoMetaJoinOperator].operator match {
            case IRJoinBy(condition :  MetaJoinCondition, left_dataset : MetaOperator, right_dataset : MetaOperator) => MetaJoinMJD3(this, condition, left_dataset, right_dataset,true, env)
          }
        mjo.asInstanceOf[NoMetaJoinOperator].operator.intermediateResult = Some(res)
        res
      }
    }else {
      if(mjo.asInstanceOf[SomeMetaJoinOperator].operator.intermediateResult.isDefined) {
        logger.info(mjo + " SomeMJoin defined - recycling partial result")
        mjo.asInstanceOf[SomeMetaJoinOperator].operator.intermediateResult.get.asInstanceOf[DataSet[FlinkMetaJoinType]]
      }
      else {
        logger.info(mjo + " not defined - executing it")
        val res =
          mjo.asInstanceOf[SomeMetaJoinOperator].operator match {
            case IRJoinBy(condition: MetaJoinCondition, leftDataset: MetaOperator, rightDataset: MetaOperator) => MetaJoinMJD3(this, condition, leftDataset, rightDataset,false, env)
          }
        mjo.asInstanceOf[SomeMetaJoinOperator].operator.intermediateResult = Some(res)
        res
      }
    }
  }
  // 

  // Meta group method

  @throws[SelectFormatException]
  def implement_mgd(mgo : MetaGroupOperator, env : ExecutionEnvironment) : DataSet[FlinkMetaGroupType2] ={
    if(mgo.intermediateResult.isDefined){
      logger.info(mgo + " defined - recycling partial result")
      mgo.intermediateResult.get.asInstanceOf[DataSet[FlinkMetaGroupType2]]
    } else {
      logger.info(mgo + " not defined - executing it")
      val res =
        mgo match {
          case IRGroupBy(groupAttributes: MetaGroupByCondition, inputDataset: MetaOperator) => MetaGroupMGD(this, groupAttributes, inputDataset, env)
        }
      mgo.intermediateResult = Some(res)
      res
    }
  }



  def generateSchemaText(schema : List[(String, PARSING_TYPE)]) : String = {
    val sb = new StringBuilder
    sb.append("<gmqlSchemaCollection name=\"DatasetName_SCHEMAS\" xmlns=\"http://genomic.elet.polimi.it/entities\">")
    sb.append("\n")
    sb.append("  <gmqlSchema  type=\"narrowPeak\">")
    sb.append("\n")
    sb.append("    <field type=\"STRING\">CHROM</field>")
    sb.append("\n")
    sb.append("    <field type=\"LONG\"> START</field>")
    sb.append("\n")
    sb.append("    <field type=\"LONG\">STOP</field>")
    sb.append("\n")
    sb.append("    <field type=\"CHAR\">STRAND</field>")
    sb.append("\n")
    schema.map((attribute) => {
      sb.append("    <field type=\"")
      sb.append(attribute._2.toString)
      sb.append("\">")
      sb.append(attribute._1)
      sb.append("</field>\n")
    })

    sb.append("  </gmqlSchema>")
    sb.append("\n")
    sb.append("</gmqlSchemaCollection>")

    sb.toString()
  }

}
