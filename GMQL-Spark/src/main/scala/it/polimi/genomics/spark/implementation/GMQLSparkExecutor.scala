package it.polimi.genomics.spark.implementation

/**
  * Created by Abdulrahman Kaitoua on 27/05/15.
  * Email: abdulrahman.kaitoua@polimi.it
  *
  */

import java.io.{BufferedWriter, OutputStreamWriter}

import it.polimi.genomics.GMQLServer.Implementation
import it.polimi.genomics.core.DataStructures.GroupMDParameters.Direction.Direction
import it.polimi.genomics.core.DataStructures.GroupMDParameters.TopParameter
import it.polimi.genomics.core.DataStructures.MetaAggregate.{MetaAggregateFunction, MetaExtension}
import it.polimi.genomics.core.DataStructures.MetaGroupByCondition.MetaGroupByCondition
import it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition
import it.polimi.genomics.core.DataStructures.RegionAggregate.{RegionExtension, RegionsToMeta}
import it.polimi.genomics.core.DataStructures.RegionCondition.RegionCondition
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.ParsingType._
import it.polimi.genomics.core._
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.profiling.Profilers.Profiler
import it.polimi.genomics.spark.implementation.MetaOperators.GroupOperator.{MetaGroupMGD, MetaJoinMJD2}
import it.polimi.genomics.spark.implementation.MetaOperators.SelectMeta._
import it.polimi.genomics.spark.implementation.MetaOperators._
import it.polimi.genomics.spark.implementation.RegionsOperators.GenometricCover.GenometricCover
import it.polimi.genomics.spark.implementation.RegionsOperators.GenometricMap._
import it.polimi.genomics.spark.implementation.RegionsOperators.SelectRegions.{ReadMEMRD, StoreGTFRD, StoreTABRD, TestingReadRD}
import it.polimi.genomics.spark.implementation.RegionsOperators._
import it.polimi.genomics.spark.implementation.loaders._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

object GMQLSparkExecutor{
  type GMQL_DATASET = (Array[(GRecordKey, Array[GValue])], Array[(Long, (String, String))], List[(String, PARSING_TYPE)])

}
class GMQLSparkExecutor(val binSize : BinSize = BinSize(), val maxBinDistance : Int = 100000, REF_PARALLILISM: Int = 20,
                        testingIOFormats:Boolean = false, sc:SparkContext,
                        outputFormat:GMQLSchemaFormat.Value = GMQLSchemaFormat.TAB,
                        outputCoordinateSystem: GMQLSchemaCoordinateSystem.Value = GMQLSchemaCoordinateSystem.Default,
                        stopContext:Boolean = true)
  extends Implementation with java.io.Serializable{


  final val ENCODING = "UTF-8"
  final val ms = System.currentTimeMillis();
  final val logger = LoggerFactory.getLogger(this.getClass)

  var fs:FileSystem = null

  def go(): Unit = {
    logger.debug(to_be_materialized.toString())
    implementation()
  }

  override def collect(variable: IRVariable): (Array[(GRecordKey, Array[GValue])], Array[(Long, (String, String))], List[(String, PARSING_TYPE)]) = {
    val metaRDD = implement_md(variable.metaDag, sc).collect()
    val regionRDD = implement_rd(variable.regionDag, sc).collect

    (regionRDD,metaRDD,variable.schema)
  }

  override def take(iRVariable: IRVariable, n: Int): (Array[(GRecordKey, Array[GValue])], Array[(Long, (String, String))], List[(String, PARSING_TYPE)]) = {
    val metaRDD = implement_md(iRVariable.metaDag, sc).collect()
    val regionRDD = implement_rd(iRVariable.regionDag, sc).groupBy(_._1._1).flatMap(_._2.take(n)).collect()

    (regionRDD,metaRDD,iRVariable.schema)
  }

  override def stop(): Unit = {
    sc.stop()
  }
  def main (args: Array[String]) {
    implementation()
  }

  def computeMetaFirst() : Seq[FlinkMetaType]= {
    List[FlinkMetaType]()
  }

  def getParser(name : String,dataset:String) : GMQLLoaderBase = {
    val path = ""

    name.toLowerCase() match {
      case "bedscoreparser" => BedScoreParser
      case "annparser" => ANNParser
      case "broadpeaksparser" => BroadPeaksParser
      case "broadpeaks" => BroadPeaksParser
      case "narrowpeakparser" => NarrowPeakParser
      case "testorderparser" =>testOrder
      case "narrowpeak" => NarrowPeakParser
      case "narrow" => NarrowPeakParser
      case "bedtabseparatedparser" => BedParser
      case "rnaseqparser" => RnaSeqParser
      case "customparser" => (new CustomParser).setSchema(dataset)
      case "broadprojparser" => BroadProjParser
      case "basicparser" => BasicParser
      case "default" =>  (new CustomParser).setSchema(dataset)
      case _ => {logger.warn("unable to find " + name + " parser, try the default one"); getParser("default",dataset)}
    }
  }

  def implementation(): Unit = {

    try {
      for (variable <- to_be_materialized) {

        val metaRDD = implement_md(variable.metaDag, sc)
        val regionRDD = implement_rd(variable.regionDag, sc)

        val variableDir = variable.metaDag.asInstanceOf[IRStoreMD].path.toString
        val MetaOutputPath =  variableDir + "/meta/"
        val RegionOutputPath = variableDir + "/files/"
        logger.debug("meta out: "+MetaOutputPath)
        logger.debug("region out "+ RegionOutputPath)

        val outputFolderName= try{
          new Path(variableDir).getName
        }
        catch{
          case _:Throwable => variableDir
        }

        val conf = new Configuration();
        val path = new org.apache.hadoop.fs.Path(RegionOutputPath);
        fs = FileSystem.get(path.toUri(), conf);

        if(testingIOFormats){
          metaRDD.map(x=>x._1+","+x._2._1 + "," + x._2._2).saveAsTextFile(MetaOutputPath)
          regionRDD.map(x=>x._1+"\t"+x._2.mkString("\t")).saveAsTextFile(RegionOutputPath)
        }


        // store schema
        storeSchema(GMQLSchema.generateSchemaXML(variable.schema,outputFolderName,outputFormat, outputCoordinateSystem),variableDir)

        // Compute Profile and store into xml files (one for web, one for optimization)
        val profile = Profiler.profile(regions = regionRDD, meta = metaRDD, sc = sc)

        try {
          val output = fs.create(new Path(variableDir + "/files/" + "profile.xml"));
          val output_web = fs.create(new Path(variableDir + "/files/" + "web_profile.xml"));

          val os = new java.io.BufferedOutputStream(output)
          val os_web = new java.io.BufferedOutputStream(output_web)

          os.write(Profiler.profileToOptXML(profile).toString().getBytes("UTF-8"))
          os_web.write(Profiler.profileToWebXML(profile).toString().getBytes("UTF-8"))

          os.close()
          os_web.close()
        } catch {
          case e: Throwable => {
            logger.error(e.getMessage)
            e.printStackTrace()
          }
        }

        fs.deleteOnExit(new Path(RegionOutputPath+"_SUCCESS"))

        fs.setVerifyChecksum(false)
        fs.listStatus(new Path(RegionOutputPath),new PathFilter {
          override def accept(path: Path): Boolean = {fs.delete(new Path(path.getParent.toString + "/."+path.getName +".crc"));true}
        })

      }
    } catch {
      case e : SelectFormatException => {
        logger.error(e.getMessage)
        e.printStackTrace()
        throw e
      }
      case e : Throwable => {
        logger.error(e.getMessage)
        e.printStackTrace()
        throw e
      }
    } finally {
      if (stopContext) {
        sc.stop()
      }
      // We need to clear the set of materialized variables if we are in interactive mode
      to_be_materialized.clear()
      logger.debug("Total Spark Job Execution Time : "+(System.currentTimeMillis()-ms).toString)
    }

  }

  @throws[SelectFormatException]
  def implement_md(mo: MetaOperator, sc : SparkContext) : RDD[DataTypes.MetaType] = {
    if(mo.intermediateResult.isDefined){
      mo.intermediateResult.get.asInstanceOf[RDD[MetaType]]
    } else {
      val res =
        mo match {
          case IRStoreMD(path, value,_) => StoreMD(this, path, value, sc)
          case IRReadMD(path, loader,_) => if (testingIOFormats)  TestingReadMD(path,loader,sc) else ReadMD(path, loader, sc)
          case IRReadMEMMD(metaRDD) => ReadMEMMD(metaRDD)
          case IRSelectMD(metaCondition, inputDataset) => SelectMD(this, metaCondition, inputDataset, sc)
          case IRPurgeMD(regionDataset, inputDataset) => inputDataset match{
//            case IRSelectMD(metaCondition1, inputDataset1) => inputDataset1 match{
//              case IRReadMD(path, loader,_) => SelectIMDWithNoIndex(this,metaCondition1,path,loader, sc)
//              case _ => SelectMD(this, metaCondition1, inputDataset1, sc)
//            }
            case _ => PurgeMD(this, regionDataset, inputDataset, sc)
          }
          case IRSemiJoin(externalMeta: MetaOperator, joinCondition: MetaJoinCondition, inputDataset: MetaOperator) => SemiJoinMD(this, externalMeta, joinCondition, inputDataset, sc)
          case IRProjectMD(projectedAttributes: Option[List[String]], metaAggregator: Option[MetaExtension], all_but_flag:Boolean, inputDataset: MetaOperator) => ProjectMD(this, projectedAttributes, metaAggregator, all_but_flag, inputDataset, sc)
          case IRUnionMD(leftDataset: MetaOperator, rightDataset: MetaOperator, leftName : String, rightName : String) => UnionMD(this, leftDataset, rightDataset, leftName, rightName, sc)
          case IRUnionAggMD(leftDataset: MetaOperator, rightDataset: MetaOperator, leftName : String, rightName : String) => UnionAggMD(this, leftDataset, rightDataset, leftName, rightName, sc)
          case IRAggregateRD(aggregator: List[RegionsToMeta], inputDataset: RegionOperator) => AggregateRD(this, aggregator, inputDataset, sc)
          case IRCombineMD(grouping: OptionalMetaJoinOperator, leftDataset: MetaOperator, rightDataset: MetaOperator, region_builder, leftName : String, rightName : String) => CombineMD(this, grouping, leftDataset, rightDataset,region_builder, leftName, rightName, sc)
          case IRDiffCombineMD(grouping: OptionalMetaJoinOperator, leftDataset: MetaOperator, rightDataset: MetaOperator, leftName : String, rightName : String) => DiffCombineMD(this, grouping, leftDataset, rightDataset, leftName, rightName, sc)
          case IRMergeMD(dataset: MetaOperator, groups: Option[MetaGroupOperator]) => MergeMD(this, dataset, groups, sc)
          case IROrderMD(ordering: List[(String, Direction)], newAttribute: String, topParameter: TopParameter, inputDataset: MetaOperator) => OrderMD(this, ordering, newAttribute, topParameter, inputDataset, sc)
          case IRGroupMD(keys: MetaGroupByCondition, aggregates: Option[List[MetaAggregateFunction]], groupName: String, inputDataset: MetaOperator, region_dataset: RegionOperator) => GroupMD(this, keys, aggregates, groupName, inputDataset, region_dataset, sc)
          case IRCollapseMD(grouping : Option[MetaGroupOperator], inputDataset : MetaOperator) => CollapseMD(this, grouping, inputDataset, sc)
        }
      mo.intermediateResult = Some(res)
      res
    }
  }

  //Region Data methods

  @throws[SelectFormatException]
  def implement_rd(ro : RegionOperator, sc : SparkContext) : RDD[DataTypes.GRECORD] = {
    if(ro.intermediateResult.isDefined){
      ro.intermediateResult.get.asInstanceOf[RDD[GRECORD]]
    } else {
      val res =
        ro match {
          case IRStoreRD(path, value,meta,schema,_) => if(outputFormat == GMQLSchemaFormat.GTF) StoreGTFRD(this, path, value,meta,schema,outputCoordinateSystem,sc) else if(outputFormat == GMQLSchemaFormat.TAB) StoreTABRD(this,path, value,meta,schema,outputCoordinateSystem,sc) else StoreRD(this, path, value, sc)
          case IRReadRD(path, loader,_) => if (testingIOFormats) TestingReadRD(path, loader, sc) else ReadRD(path, loader, sc)
          case IRReadMEMRD(metaRDD) => ReadMEMRD(metaRDD)
          case IRSelectRD(regionCondition: Option[RegionCondition], filteredMeta: Option[MetaOperator], inputDataset: RegionOperator) =>
            inputDataset match {
              case IRReadRD(path, loader,_) =>
                if(filteredMeta.isDefined) {
                  SelectIRD(this, regionCondition, filteredMeta, loader,path,None, sc)
                }
                else SelectRD(this, regionCondition, filteredMeta, inputDataset, sc)
              case _ => SelectRD(this, regionCondition, filteredMeta, inputDataset, sc)
            }
          case IRPurgeRD(metaDataset: MetaOperator, inputDataset: RegionOperator) => PurgeRD(this, metaDataset, inputDataset, sc)
          case irCover:IRRegionCover => GenometricCover(this, irCover.cover_flag, irCover.min, irCover.max, irCover.aggregates, irCover.groups, irCover.input_dataset,binSize.Cover, sc)
          case IRUnionRD(schemaReformatting: List[Int], leftDataset: RegionOperator, rightDataset: RegionOperator) => UnionRD(this, schemaReformatting, leftDataset, rightDataset, sc)
          case IRMergeRD(dataset: RegionOperator, groups: Option[MetaGroupOperator]) => MergeRD(this, dataset, groups, sc)
          case IRGroupRD(groupingParameters: Option[List[GroupRDParameters.GroupingParameter]], aggregates: Option[List[RegionAggregate.RegionsToRegion]], regionDataset: RegionOperator) => GroupRD(this, groupingParameters, aggregates, regionDataset, sc)
          case IROrderRD(ordering: List[(Int, Direction)], topPar: TopParameter, inputDataset: RegionOperator) => OrderRD(this, ordering: List[(Int, Direction)], topPar, inputDataset, sc)
          case irJoin:IRGenometricJoin => GenometricJoin4TopMin3(this, irJoin.metajoin_condition, irJoin.join_condition, irJoin.region_builder, irJoin.left_dataset, irJoin.right_dataset,irJoin.join_on_attributes,binSize.Join,maxBinDistance, sc)
          case irMap:IRGenometricMap => GenometricMap71(this, irMap.grouping, irMap.aggregates, irMap.reference, irMap.samples,binSize.Map,REF_PARALLILISM, sc)
          case IRDifferenceRD(metaJoin: OptionalMetaJoinOperator, leftDataset: RegionOperator, rightDataset: RegionOperator,exact:Boolean) => GenometricDifference(this, metaJoin, leftDataset, rightDataset,exact, sc)
          case IRProjectRD(projectedValues: Option[List[Int]], tupleAggregator: Option[List[RegionExtension]], inputDataset: RegionOperator, inputDatasetMeta: MetaOperator) => ProjectRD(this, projectedValues, tupleAggregator, inputDataset,inputDatasetMeta, sc)

        }
      ro.intermediateResult = Some(res)
      res
    }
  }
  @throws[SelectFormatException]
  def implement_mgd(mgo : MetaGroupOperator, sc : SparkContext) : RDD[FlinkMetaGroupType2] ={
    if(mgo.intermediateResult.isDefined){
      mgo.intermediateResult.get.asInstanceOf[RDD[FlinkMetaGroupType2]]
    } else {
      val res =
        mgo match {
          case IRGroupBy(groupAttributes: MetaGroupByCondition, inputDataset: MetaOperator) => MetaGroupMGD(this, groupAttributes, inputDataset, sc)
        }
      mgo.intermediateResult = Some(res)
      res
    }
  }

  @throws[SelectFormatException]
  def implement_mjd(mjo : OptionalMetaJoinOperator, sc : SparkContext) : RDD[SparkMetaJoinType] = {
    if(mjo.isInstanceOf[NoMetaJoinOperator]){
      if(mjo.asInstanceOf[NoMetaJoinOperator].operator.intermediateResult.isDefined)
        mjo.asInstanceOf[NoMetaJoinOperator].operator.intermediateResult.get.asInstanceOf[RDD[SparkMetaJoinType]]
      else {
        val res =
          mjo.asInstanceOf[NoMetaJoinOperator].operator match {
            case IRJoinBy(condition :  MetaJoinCondition, left_dataset : MetaOperator, right_dataset : MetaOperator) => MetaJoinMJD2(this, condition, left_dataset, right_dataset,true, sc)
          }
        mjo.asInstanceOf[NoMetaJoinOperator].operator.intermediateResult = Some(res)
        res
      }
    }else {
      if(mjo.asInstanceOf[SomeMetaJoinOperator].operator.intermediateResult.isDefined)
        mjo.asInstanceOf[SomeMetaJoinOperator].operator.intermediateResult.get.asInstanceOf[RDD[SparkMetaJoinType]]
      else {
        val res =
          mjo.asInstanceOf[SomeMetaJoinOperator].operator match {
            case IRJoinBy(condition: MetaJoinCondition, leftDataset: MetaOperator, rightDataset: MetaOperator) => MetaJoinMJD2(this, condition, leftDataset, rightDataset,false, sc)
          }
        mjo.asInstanceOf[SomeMetaJoinOperator].operator.intermediateResult = Some(res)
        res
      }
    }
  }

  //Other usefull methods

  def castDoubleOrString(value : Any) : Any = {
    try{
      value.toString.toDouble
    } catch {
      case e : Throwable => value.toString
    }
  }

  def storeSchema(schema: String, path : String)= {
    val schemaPath = path+"/files/schema.xml"
    val br = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(schemaPath)), "UTF-8"));
    br.write(schema)
    br.close()
  }


}
