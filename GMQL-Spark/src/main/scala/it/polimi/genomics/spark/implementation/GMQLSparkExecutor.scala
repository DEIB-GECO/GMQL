package it.polimi.genomics.spark.implementation

/**
 * Created by Abdulrahman Kaitoua on 27/05/15.
 * Email: abdulrahman.kaitoua@polimi.it
 *
 */

import java.io.{BufferedWriter, OutputStreamWriter, PrintWriter}

import it.polimi.genomics.GMQLServer.Implementation
import it.polimi.genomics.core.DataStructures.GroupMDParameters.Direction.Direction
import it.polimi.genomics.core.DataStructures.GroupMDParameters.TopParameter
import it.polimi.genomics.core.DataStructures.MetaAggregate.MetaAggregateStruct
import it.polimi.genomics.core.DataStructures.MetaGroupByCondition.MetaGroupByCondition
import it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition
import it.polimi.genomics.core.DataStructures.RegionAggregate.{RegionExtension, RegionsToMeta}
import it.polimi.genomics.core.DataStructures.RegionCondition.RegionCondition
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.{BinSize, DataTypes, GMQLLoaderBase, GMQLOutputFormat}
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.ParsingType._
import it.polimi.genomics.core.exception.SelectFormatException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import it.polimi.genomics.spark.implementation.MetaOperators.GroupOperator.{MetaGroupMGD, MetaJoinMJD2}
import it.polimi.genomics.spark.implementation.MetaOperators.SelectMeta._
import it.polimi.genomics.spark.implementation.MetaOperators._
import it.polimi.genomics.spark.implementation.RegionsOperators.GenometricCover.GenometricCover1
import it.polimi.genomics.spark.implementation.RegionsOperators.GenometricMap._
import it.polimi.genomics.spark.implementation.RegionsOperators.SelectRegions.TestingReadRD
import it.polimi.genomics.spark.implementation.RegionsOperators._
import it.polimi.genomics.spark.implementation.loaders._

import scala.collection.JavaConverters._
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.Map
import scala.xml.Elem

class GMQLSparkExecutor(val binSize : BinSize = BinSize(), val maxBinDistance : Int = 100000, REF_PARALLILISM: Int = 20,
                        testingIOFormats:Boolean = false, sc:SparkContext,
                        outputFormat:GMQLOutputFormat.Value = GMQLOutputFormat.TAB)
  extends Implementation with java.io.Serializable{


  final val ENCODING = "UTF-8"
  final val ms = System.currentTimeMillis();
  final val logger = LoggerFactory.getLogger(this.getClass)

  var fs:FileSystem = null

  def go(): Unit = {
    logger.info(to_be_materialized.toString())
    implementation()
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

        val variableDir =variable.metaDag.asInstanceOf[IRStoreMD].path.toString
        val MetaOutputPath =  variableDir + "/meta/"
        val RegionOutputPath = variableDir + "/exp/"
        logger.debug("meta out: "+MetaOutputPath)
        logger.debug("region out "+ RegionOutputPath)


        val conf = new Configuration();
        val path = new org.apache.hadoop.fs.Path(RegionOutputPath);
        fs = FileSystem.get(path.toUri(), conf);

        if(testingIOFormats){
          metaRDD.map(x=>x._1+","+x._2._1 + "," + x._2._2).saveAsTextFile(MetaOutputPath)
          regionRDD.map(x=>x._1+"\t"+x._2.mkString("\t")).saveAsTextFile(RegionOutputPath)

        }else {
//          val fsRegDir = FSR_Utilities.gethdfsConfiguration().get("fs.defaultFS")+
//            General_Utilities().getHDFSRegionDir(General_Utilities().USERNAME)
//          val localRegDir = General_Utilities().getLocalRegionDir(General_Utilities().USERNAME)

          val MetaOutputPath =
//            if (General_Utilities().MODE == General_Utilities().HDFS)
//            fsRegDir + variableDir + "/meta/"
//          else localRegDir +
            variableDir + "/meta/"

          val RegionOutputPath =
//            if (General_Utilities().MODE == General_Utilities().HDFS)
//            fsRegDir + variable.regionDag.asInstanceOf[IRStoreRD].path.toString + "/exp/"
//          else localRegDir +
            variableDir + "/exp/"

          logger.info(MetaOutputPath)
          logger.info(RegionOutputPath)
          logger.info(metaRDD.toDebugString)
          logger.info(regionRDD.toDebugString)

          val outputFolderName= try{
            new Path(variableDir).getName
          }
          catch{
            case _:Throwable => variableDir
          }

          val outSample = "S"

          val Ids = metaRDD.keys.distinct()
          val newIDS: Map[Long, Long] = Ids.zipWithIndex().collectAsMap()
          val newIDSbroad = sc.broadcast(newIDS)

          val regionsPartitioner = new HashPartitioner(Ids.count.toInt)

          val keyedRDD = if(!(outputFormat == GMQLOutputFormat.GTF)){
             regionRDD.map(x => (outSample+"_"+ "%05d".format(newIDSbroad.value.get(x._1._1).get)+".gdm",
               x._1._2 + "\t" + x._1._3 + "\t" + x._1._4 + "\t" + x._1._5 + "\t" + x._2.mkString("\t")))
               .partitionBy(regionsPartitioner).mapPartitions(x=>x.toList.sortBy{s=> val data = s._2.split("\t"); (data(0),data(1).toLong,data(2).toLong)}.iterator)
          }else {
            val jobname = outputFolderName
            val score = variable.schema.zipWithIndex.filter(x => x._1._1.toLowerCase().equals("score"))
            val scoreIndex = if (score.size > 0) score(0)._2 else -1
            regionRDD.map { x =>
              val values = variable.schema.zip(x._2).flatMap { s => if (s._1._1.equals("score")) None else Some(s._1._1 + " \"" + s._2 + "\";") }.mkString(" ")
              (outSample + "_" + "%05d".format(newIDSbroad.value.get(x._1._1).get) + ".gtf",
                x._1._2 //chrom
                  //                  + "\t" + jobname.substring(jobname.lastIndexOf("_")+1,jobname.length)   //variable name
                  + "\t" + "GMQL" //variable name
                  + "\t" + "Region"
                  + "\t" + x._1._3 + "\t" + x._1._4 + "\t" //start , stop
                  + {
                  if (scoreIndex >= 0) x._2(scoreIndex) else "0.0"
                } //score
                  + "\t" + (if (x._1._5.equals('*')) '.' else x._1._5) + "\t" //strand
                  + "." //frame
                  + "\t" + values
              )
            }.partitionBy(regionsPartitioner)
              .mapPartitions(x=>x.toList.sortBy{s=> val data = s._2.split("\t"); (data(0),data(3).toLong,data(4).toLong)}.iterator)
          }

          writeMultiOutputFiles.saveAsMultipleTextFiles(keyedRDD, RegionOutputPath)


          val metaKeyValue = if(!(outputFormat == GMQLOutputFormat.GTF)){
            metaRDD.map(x => (outSample+"_"+ "%05d".format(newIDSbroad.value.get(x._1).get) + ".gdm.meta", x._2._1 + "\t" + x._2._2)).repartition(1).sortBy(x=>(x._1,x._2))
          }else{
            metaRDD.map(x => (outSample+"_"+ "%05d".format(newIDSbroad.value.get(x._1)) + ".gtf.meta", x._2._1 + "\t" + x._2._2)).repartition(1).sortBy(x=>(x._1,x._2))
          }
          writeMultiOutputFiles.saveAsMultipleTextFiles(metaKeyValue, MetaOutputPath)

          writeMultiOutputFiles.fixOutputMetaLocation(MetaOutputPath)
          //        fixOutputNamingLocation(RegionOutputPath,"0")
          //        fixOutputNamingLocation(MetaOutputPath,"0.meta")
        }
        storeSchema(generateSchema(variable.schema),variableDir)
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
      sc.stop()
      logger.info("Total Spark Job Execution Time : "+(System.currentTimeMillis()-ms).toString)
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
          case IRSelectMD(metaCondition, inputDataset) =>
            inputDataset match {
              case IRReadMD(path, loader,_) => SelectIMDWithNoIndex(this, metaCondition, path, loader, sc)
              case _ => SelectMD(this, metaCondition, inputDataset, sc)
            }
          case IRPurgeMD(regionDataset, inputDataset) => inputDataset match{
            case IRSelectMD(metaCondition1, inputDataset1) => inputDataset1 match{
              case IRReadMD(path, loader,_) => SelectIMDWithNoIndex(this,metaCondition1,path,loader, sc)
              case _ => SelectMD(this, metaCondition1, inputDataset1, sc)
            }
            case _ => PurgeMD(this, regionDataset, inputDataset, sc)
          }
          case IRSemiJoin(externalMeta: MetaOperator, joinCondition: MetaJoinCondition, inputDataset: MetaOperator) => SemiJoinMD(this, externalMeta, joinCondition, inputDataset, sc)
          case IRProjectMD(projectedAttributes: Option[List[String]], metaAggregator: Option[MetaAggregateStruct], inputDataset: MetaOperator) => ProjectMD(this, projectedAttributes, metaAggregator, inputDataset, sc)
          case IRUnionMD(leftDataset: MetaOperator, rightDataset: MetaOperator, leftName : String, rightName : String) => UnionMD(this, leftDataset, rightDataset, leftName, rightName, sc)
          case IRUnionAggMD(leftDataset: MetaOperator, rightDataset: MetaOperator, leftName : String, rightName : String) => UnionAggMD(this, leftDataset, rightDataset, leftName, rightName, sc)
          case IRAggregateRD(aggregator: List[RegionsToMeta], inputDataset: RegionOperator) => AggregateRD(this, aggregator, inputDataset, sc)
          case IRCombineMD(grouping: OptionalMetaJoinOperator, leftDataset: MetaOperator, rightDataset: MetaOperator, leftName : String, rightName : String) => CombineMD(this, grouping, leftDataset, rightDataset, leftName, rightName, sc)
          case IRMergeMD(dataset: MetaOperator, groups: Option[MetaGroupOperator]) => MergeMD(this, dataset, groups, sc)
          case IROrderMD(ordering: List[(String, Direction)], newAttribute: String, topParameter: TopParameter, inputDataset: MetaOperator) => OrderMD(this, ordering, newAttribute, topParameter, inputDataset, sc)
          case IRGroupMD(keys: MetaGroupByCondition, aggregates: List[RegionsToMeta], groupName: String, inputDataset: MetaOperator, region_dataset: RegionOperator) => GroupMD(this, keys, aggregates, groupName, inputDataset, region_dataset, sc)
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
          case IRStoreRD(path, value,_) => StoreRD(this, path, value, sc)
          case IRReadRD(path, loader,_) => if (testingIOFormats) TestingReadRD(path, loader, sc) else ReadRD(path, loader, sc)
          case IRSelectRD(regionCondition: Option[RegionCondition], filteredMeta: Option[MetaOperator], inputDataset: RegionOperator) =>
            inputDataset match {
              case IRReadRD(path, loader,_) =>
                if(filteredMeta.isDefined) {
//                  if(new LFSRepository().DSExists(new IRDataSet(General_Utilities().USERNAME,List[(String,PARSING_TYPE)]().asJava),path(0))){
//                    val schema = path(0)
//                    SelectIRD(this, regionCondition, filteredMeta, loader,path,Some(schema), sc)
//                  }
//                  else
                    SelectIRD(this, regionCondition, filteredMeta, loader,path,None, sc)
                }
                else SelectRD(this, regionCondition, filteredMeta, inputDataset, sc)
              case _ => SelectRD(this, regionCondition, filteredMeta, inputDataset, sc)
            }
          case IRPurgeRD(metaDataset: MetaOperator, inputDataset: RegionOperator) => PurgeRD(this, metaDataset, inputDataset, sc)
//            inputDataset match {
//            case IRSelectRD(regionCondition1: Option[RegionCondition], filteredMeta1: Option[MetaOperator], inputDataset1: RegionOperator) => inputDataset1 match {
//              case IRReadRD(path, loader) => if(filteredMeta1.isDefined)
//                if(Utilities.getInstance().checkDSName(Utilities.USERNAME,path(0))){
//                  val schema = Utilities.getInstance().RepoDir+Utilities.USERNAME+"/schema/"+path(0)+".schema"
//                  SelectIRD(this, regionCondition1, filteredMeta1, loader,path,Some(schema), sc)
//                }else
//                SelectIRD(this, regionCondition1, filteredMeta1, loader,path,None, sc)
//              else
//                SelectRD(this, regionCondition1, filteredMeta1, inputDataset, sc)
//              case _ => SelectRD(this, regionCondition1, filteredMeta1, inputDataset, sc)
//            }
//            case _ => PurgeRD(this, metaDataset, inputDataset, sc)
//          } RD(projectedValues: Option[List[Int]], tupleAggregator: Option[List[RegionExtension]], inputDataset: RegionOperator) => ProjectRD(this, projectedValues, tupleAggregator, inputDataset, sc)
          case irCover:IRRegionCover => GenometricCover1(this, irCover.cover_flag, irCover.min, irCover.max, irCover.aggregates, irCover.groups, irCover.input_dataset,2000/*irCover.binSize.getOrElse(defaultBinSize)*/, sc)
          case IRUnionRD(schemaReformatting: List[Int], leftDataset: RegionOperator, rightDataset: RegionOperator) => UnionRD(this, schemaReformatting, leftDataset, rightDataset, sc)
          case IRMergeRD(dataset: RegionOperator, groups: Option[MetaGroupOperator]) => MergeRD(this, dataset, groups, sc)
          case IRGroupRD(groupingParameters: Option[List[GroupRDParameters.GroupingParameter]], aggregates: Option[List[RegionAggregate.RegionsToRegion]], regionDataset: RegionOperator) => GroupRD(this, groupingParameters, aggregates, regionDataset, sc)
          case IROrderRD(ordering: List[(Int, Direction)], topPar: TopParameter, inputDataset: RegionOperator) => OrderRD(this, ordering: List[(Int, Direction)], topPar, inputDataset, sc)
          case irJoin:IRGenometricJoin => GenometricJoin4TopMin2(this, irJoin.metajoin_condition, irJoin.join_condition, irJoin.region_builder, irJoin.left_dataset, irJoin.right_dataset,50000/*irJoin.binSize.getOrElse(defaultBinSize)*/,/*irJoin.binSize.getOrElse(defaultBinSize)**/maxBinDistance, sc)
          case irMap:IRGenometricMap => GenometricMap71(this, irMap.grouping, irMap.aggregates, irMap.reference, irMap.samples,50000/*irMap.binSize.getOrElse(defaultBinSize)*/,REF_PARALLILISM, sc)
          case IRDifferenceRD(metaJoin: OptionalMetaJoinOperator, leftDataset: RegionOperator, rightDataset: RegionOperator) => GenometricDifference(this, metaJoin, leftDataset, rightDataset, sc)
          case IRProjectRD(projectedValues: Option[List[Int]], tupleAggregator: Option[List[RegionExtension]], inputDataset: RegionOperator) => ProjectRD(this, projectedValues, tupleAggregator, inputDataset, sc)

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
    //    if (schema.isDefined) {
    val schemaHeader = schema//.get
//    val dir = new java.io.File(path);
//    val fs = FSR_Utilities.getFileSystem
//    val parentDir = if(path.startsWith("hdfs")) {
//        new java.io.File((new Path(path).getName));
//      } else new java.io.File(dir.getPath)

//    if (!parentDir.exists) parentDir.mkdirs()
    //TODO when taking off GMQL V1 then refine these conditions
    //Where to store the schema file. In Case GMQL V2 only, we need to store it only on local
//    if ((new java.io.File("/" + parentDir.getPath)).exists())
//      new PrintWriter(parentDir.getPath + "/test.schema") { write(schemaHeader); close }
//    else if (General_Utilities().MODE == General_Utilities().HDFS){
      val schemaPath = path+"/exp/test.schema"
      val br = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(schemaPath)), "UTF-8"));
      br.write(schema);
      br.close();
//      fs.close();
//    } else if (General_Utilities().MODE == General_Utilities().LOCAL)
//      new PrintWriter(General_Utilities().getSchemaDir(General_Utilities().USERNAME) + parentDir.getPath + ".schema") { write(schemaHeader); close }

//    if(General_Utilities().MODE == General_Utilities().HDFS){
//      // HDFS verison is needed only for GMQL V1
//      val localPath = General_Utilities().getSchemaDir(General_Utilities().USERNAME) + parentDir.getPath + ".schema"
//      try{
//       new PrintWriter(localPath) { write(schemaHeader); close }
//      }catch{
//        case ex:Exception => logger.warn("schema data is not updated in the main repository manager. ")
//      }
//    }
  }

  def generateSchema(schema : List[(String, PARSING_TYPE)]): String ={
    val schemaPart = if(outputFormat == GMQLOutputFormat.GTF) {
      "\t<gmqlSchema type=\"gtf\">\n"+
      "           <field type=\"STRING\">seqname</field>\n" +
        "           <field type=\"STRING\">source</field>\n"+
        "           <field type=\"STRING\">feature</field>\n"+
        "           <field type=\"LONG\">start</field>\n"+
        "           <field type=\"LONG\">end</field>\n"+
        "           <field type=\"DOUBLE\">score</field>\n"+
        "           <field type=\"CHAR\">strand</field>\n"+
      "           <field type=\"STRING\">frame</field>"

    }else {
  "      <gmqlSchema type=\"Peak\">\n" +
  "           <field type=\"STRING\">chr</field>\n" +
  "           <field type=\"LONG\">left</field>\n" +
  "           <field type=\"LONG\">right</field>\n" +
  "           <field type=\"CHAR\">strand</field>"
    }

    val schemaHeader =
    //http://www.bioinformatics.deib.polimi.it/GMQL/
      "<?xml version='1.0' encoding='UTF-8'?>\n" +
        "<gmqlSchemaCollection name=\"DatasetName_SCHEMAS\" xmlns=\"http://genomic.elet.polimi.it/entities\">\n" +
        schemaPart +"\n"+
        schema.flatMap{x =>
            if(outputFormat == GMQLOutputFormat.GTF && x._1.toLowerCase() == "score") None
            else Some("           <field type=\"" + x._2.toString + "\">" + x._1 + "</field>")
        }.mkString("\n") +
          "\n\t</gmqlSchema>\n" +
     "</gmqlSchemaCollection>"

      schemaHeader
  }
}
