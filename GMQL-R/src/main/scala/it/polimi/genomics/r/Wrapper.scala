package it.polimi.genomics.r

import java.io.FileNotFoundException
import java.util.concurrent.atomic.AtomicLong

import it.polimi.genomics.GMQLServer.{DefaultMetaAggregateFactory, DefaultRegionsToMetaFactory, DefaultRegionsToRegionFactory, GmqlServer}
import it.polimi.genomics.core.DataStructures.CoverParameters._
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor.GMQL_DATASET
import it.polimi.genomics.core.DataStructures.GroupMDParameters.Direction.Direction
import it.polimi.genomics.core.DataStructures.GroupMDParameters._
import it.polimi.genomics.core.DataStructures.GroupRDParameters.{FIELD, GroupingParameter}
import it.polimi.genomics.core.DataStructures.JoinParametersRD._
import it.polimi.genomics.core.DataStructures.JoinParametersRD.RegionBuilder.RegionBuilder
import it.polimi.genomics.core.DataStructures.MetaAggregate.{MetaAggregateFunction, MetaExtension}
import it.polimi.genomics.core.DataStructures.MetaGroupByCondition.MetaGroupByCondition
import it.polimi.genomics.core.DataStructures.{IRDataSet, IROperator, IRReadMD, IRReadRD, IRVariable, MetaOperator, RegionOperator}
import it.polimi.genomics.core.DataStructures.MetaJoinCondition._
import it.polimi.genomics.core.DataStructures.MetadataCondition.MetadataCondition
import it.polimi.genomics.core.DataStructures.RegionAggregate.{RegionFunction, RegionsToMeta, RegionsToRegion}
import it.polimi.genomics.core.DataStructures.RegionCondition.RegionCondition
import it.polimi.genomics.core.{GValue, _}
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import it.polimi.genomics.spark.implementation.loaders._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Created by simone on 21/03/17.
  */


object Wrapper {

  var GMQL_server: GmqlServer = _
  var Spark_context: SparkContext = _
  var change_processing_possible= true
  var remote_processing: Boolean = false
  var outputformat: GMQLSchemaFormat.Value = _
  var rest_manager = RESTManager
  var materialize_list: ListBuffer[IRVariable] = new ListBuffer[IRVariable]

  //thread safe counter for unique string pointer to dataset
  //'cause we could have two pointer at the same dataset and we
  //can not distinguish them
  //example:
  // R shell
  //r = readDataset("/Users/simone/Downloads/job_filename_guest_new14_20170316_162715_DATA_SET_VAR")
  //r1 = readDataset("/Users/simone/Downloads/job_filename_guest_new14_20170316_162715_DATA_SET_VAR")
  //are mapped in vv with the same key but different in R

  var counter: AtomicLong = new AtomicLong(0)
  var dataset_index: AtomicLong = new AtomicLong(0)

  // probably not needed use materialize_list.length()
  var materialize_count: AtomicLong = new AtomicLong(0)

  var mem_meta: Array[Array[String]] = _
  var mem_regions: Array[Array[String]] = _
  var mem_schema: Array[Array[String]] = _

  var vv: Map[String, IRVariable] = Map[String, IRVariable]()
  var datasetQueue:ArrayBuffer[Array[String]] = new ArrayBuffer[Array[String]]()

  def getGvalueArray(value: Array[String]): Array[GValue] = {
    val temp: ArrayBuffer[GValue] = new ArrayBuffer[GValue]()
    for (elem <- value) {
      if (elem.equalsIgnoreCase("."))
        temp += GString(".")
      else if(elem.equalsIgnoreCase("+"))
        temp += GString("+")
      else if(elem.equalsIgnoreCase("-"))
        temp += GString("-")
      else if (elem matches "[\\+\\-0-9.e]+")
        temp += GDouble(elem.toDouble)
      else
      {
        if (elem.equalsIgnoreCase("NA"))
          temp += GString(".")
        else
          temp += GString(elem)
      }
    }
    temp.toArray
  }

  def initGMQL(output_format: String, remote_proc: Boolean): Unit = {
    val spark_conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("GMQL-R")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.executor.memory", "6g")
      .set("spark.driver.memory", "2g")
    Spark_context = SparkContext.getOrCreate(spark_conf)

    outputformat = outputFormat(output_format)

    val executor = new GMQLSparkExecutor(sc = Spark_context, outputFormat = outputformat)
    GMQL_server = new GmqlServer(executor)

    change_processing_possible=true
    remote_processing = remote_proc
    println("GMQL Server is up")
  }

  def remote_processing(remote: Boolean): String = {

    if(GMQL_server == null)
      return "invoke init_gmql() first"

    if(change_processing_possible)
    {
      remote_processing = remote
      if (!remote_processing)
        "Remote processing off"
      else
        "Remote processing On"
    }
    else
      "Cannot change processing mode"
  }

  def is_remote_processing(): Boolean = {
    remote_processing
  }

  def outputFormat(format: String): GMQLSchemaFormat.Value = {
    format match {
      case "TAB" => GMQLSchemaFormat.TAB
      case "GTF" => GMQLSchemaFormat.GTF
      case "VCF" => GMQLSchemaFormat.VCF
      case "COLLECT" => GMQLSchemaFormat.COLLECT
      case _ => GMQLSchemaFormat.TAB
    }
  }

  def coordSystem(coord: String): GMQLSchemaCoordinateSystem.Value = {
    coord match {
      case "default" => GMQLSchemaCoordinateSystem.Default
      case "0-based" => GMQLSchemaCoordinateSystem.ZeroBased
      case "1-based" => GMQLSchemaCoordinateSystem.OneBased
      case _ => GMQLSchemaCoordinateSystem.Default
    }
  }

  def stopGMQL(): Unit =
  {
    if(GMQL_server!=null) {
      Spark_context.stop()
      GMQL_server = null
    }
    else
      println("GMQL Server has not been initialized yet")
  }

  def readDataset(data_input_path: String, parser_name: String, is_local: Boolean, is_GMQL:Boolean,
                  schema: Array[Array[String]],path_schema_XML:String,coordSys:String,
                  formatType:String): Array[String] =
  {
    if(GMQL_server == null)
      return Array("1","invoke init_gmql() first")

    var parser: BedParser = null
    var out_p = ""
    var loc_path = ""
    var remote_path = ""
    var data_path = data_input_path

    if (is_local && is_GMQL)
    {
      val name_local:Array[String] = data_path.split("/")
      val last_name = name_local(name_local.length-1)
      if(last_name != "files")
        data_path = data_path + "/files"
    }

    parser_name match {
      //case "BEDPARSER" => parser = BedParser.asInstanceOf[BedParser]
      case "ANNPARSER" => parser = ANNParser.asInstanceOf[BedParser]
      case "BROADPEAKPARSER" => parser = BroadPeaksParser.asInstanceOf[BedParser]
      case "BASICPARSER" => parser = BasicParser.asInstanceOf[BedParser]
      case "NARROWPEAKPARSER" => parser = NarrowPeakParser.asInstanceOf[BedParser]
      case "RNASEQPARSER" => parser = RnaSeqParser.asInstanceOf[BedParser]
      case "CUSTOMPARSER" => {
        if (is_local)
        {
          parser = new CustomParser()
          try {
            parser.asInstanceOf[CustomParser].setSchema(path_schema_XML)
          }
          catch {
            case fe: FileNotFoundException => return Array("1",fe.getMessage)
          }
        }
        else {
          if (schema != null) {
            parser = createParser(schema,coordSys,formatType)
          }
          else
            return Array("1","No schema defined")
        }
      }
      case _ => return Array("1","No parser defined")
    }

    if(is_local) {
      loc_path = data_path
      remote_path = null
    }
    else {

      loc_path=null
      val owner_dataset:Array[String] = data_path.split("\\.")
      val dataset = owner_dataset(owner_dataset.length-1)
      remote_path = dataset
      data_path = dataset
    }

    val dataAsTheyAre = GMQL_server.READ(data_path).USING(parser)
    val index = counter.getAndIncrement()
    out_p = "dataset" + index
    vv = vv + (out_p -> dataAsTheyAre)
    val elem_map =  Array(loc_path,remote_path,parser_name,is_GMQL.toString)

    this.datasetQueue += elem_map

    Array("0",out_p)
  }

  def createParser(schema: Array[Array[String]],cSystem:String,formatType:String): BedParser = {

    var chr_index = 0
    var start_index = 0
    var end_index = 0
    var strand_index = 0
    val schemaList = new ListBuffer[(Int, ParsingType.PARSING_TYPE)]()
    val schemaList_name = new ListBuffer[(String, ParsingType.PARSING_TYPE)]()


    for (i <- schema.indices) {
      var name = schema(i)(0).toLowerCase
      var parse_type = schema(i)(1)
      name match {
        case "seqname" | "seqnames" | "chr" | "chrom" => chr_index = i
        case "start" | "left" => start_index = i
        case "end" | "right" | "stop" => end_index = i
        case "strand" => strand_index = i
        case _ => {
          val pt = getParsingTypeFromString(parse_type)
          schemaList += ((i, pt))
          schemaList_name += ((name,pt))
        }
      }
    }


    val parser = new BedParser("\t", chr_index, start_index, end_index, Some(strand_index), Some(schemaList.toArray))
    parser.schema = schemaList_name.toList

    val gType = outputFormat(formatType.toUpperCase)
    parser.parsingType = gType

    gType match{
      case GMQLSchemaFormat.GTF =>
        parser.calculateMapParameters(Some(schemaList_name.map(_._1).tail.tail.tail.tail))
        parser.coordinateSystem = coordSystem(cSystem.toLowerCase)
      case GMQLSchemaFormat.TAB =>
        parser.coordinateSystem = coordSystem("0-based")
    }

    parser
  }

  def getParsingTypeFromString(parsingType: String): PARSING_TYPE = {

    val pType = parsingType.toUpperCase()
    pType match {
      case "STRING" => ParsingType.STRING
      case "CHAR" => ParsingType.STRING
      case "CHARACTER" => ParsingType.STRING
      case "LONG" => ParsingType.DOUBLE
      case "INTEGER" => ParsingType.DOUBLE
      case "INT" => ParsingType.DOUBLE
      case "BOOLEAN" => ParsingType.STRING
      case "BOOL" => ParsingType.STRING
      case "FACTOR" => ParsingType.STRING
      case "NUMERIC" => ParsingType.DOUBLE
      case "DOUBLE" => ParsingType.DOUBLE
      case _ => ParsingType.DOUBLE
    }
  }

  def read(meta: Array[Array[String]], regions: Array[Array[String]],
           schema: Array[Array[String]],coordSys:String, formatType:String): String = {

    val metaDS = Spark_context.parallelize(meta.map { x => (x(0).toLong, (x(1), x(2))) })

    val regionDS = Spark_context.parallelize(regions.map {
      x => (new GRecordKey(x(0).toLong, x(1), x(2).toLong, x(3).toLong, x(4).toCharArray.head), getGvalueArray(x.drop(4)))
    })
    val parser = createParser(schema,coordSys,formatType)


    val dataAsTheyAre = GMQL_server.READ("").USING(metaDS, regionDS, parser.schema)

    val index = counter.getAndIncrement()
    val out_p = "dataset" + index
    vv = vv + (out_p -> dataAsTheyAre)
    out_p

  }

  def materialize(data_to_materialize: String, data_output_path: String): Array[String] = {

    if (vv.get(data_to_materialize).isEmpty)
      return Array("1","No valid Data to materialize")

    val materialize = vv(data_to_materialize)
    var metaDAG = materialize.metaDag
    var regDAG = materialize.regionDag
    if(remote_processing)
    {
      for (elem <- datasetQueue)
      {
        //var remote = elem(1)
        var local = elem(0)
        if(local!=null)
        {
          val name_local:Array[String] = local.split("/")
          val last_name = name_local(name_local.length-2)
          //upload
          modify_DAG(metaDAG,local,last_name)
          modify_DAG(regDAG,local,last_name)
          elem(1) = last_name
        }
      }
    }
    else
    {
      for (elem <- datasetQueue)
      {
        var remote = elem(1)
        //var local = elem(0)
        if(remote!=null)
        {
          val dir_out = data_output_path + "/" + remote +"/files"
          //download
          elem(0) = data_output_path
          modify_DAG(metaDAG, remote, dir_out)
          modify_DAG(regDAG, remote, dir_out)
        }
      }
    }
    GMQL_server setOutputPath data_output_path MATERIALIZE materialize
    //materialize_list += materialize
    materialize_count.getAndIncrement()
    change_processing_possible=false

    Array("0","Materialized")
  }


  def modify_DAG(dag: IROperator, source: String, dest: String): Unit = {
    dag match {
      case x: IRReadMD[_,_,_,_] =>
        if(x.dataset.position == source) {
          val newDataset = IRDataSet(dest, x.dataset.schema)
          x.dataset = newDataset
          x.paths = List(dest)
        }
      case x: IRReadRD[_,_,_,_] =>
        if(x.dataset.position == source) {
          val newDataset = IRDataSet(dest, x.dataset.schema)
          x.dataset = newDataset
          x.paths = List(dest)
        }
      case _ =>
    }
    dag.getOperatorList.map(operator => modify_DAG(operator, source, dest))
  }

  def execute(): Array[String] =
  {
    if(outputformat == GMQLSchemaFormat.COLLECT)
      return Array("1","No execute() available, you choose memory as output," +
        " use take() function instead")

    if (materialize_count.get() <= 0)
      Array("1","You have to materialize before")
    else
    {
      if(remote_processing)
      {
        val dagW = DAGWrapper(GMQL_server.materializationList.toList)
        val base64DAG:String  = DAGSerializer.serializeDAG(dagW)
        change_processing_possible = true
        GMQL_server.materializationList.clear()
        return Array("0",base64DAG)
      }
      else
      {
        GMQL_server.run()
        materialize_count.set(0)
      }
      change_processing_possible = true
      GMQL_server.materializationList.clear()
      Array("0","Executed")
    }
  }

  def take(data_to_take: String, how_many: Int): Array[String] = {
    if (vv.get(data_to_take).isEmpty)
      return Array("1","No valid Data to take")

    var output: Any = null
    val taken = vv(data_to_take)

    if (how_many == 0)
      output = GMQL_server.setOutputPath("").COLLECT(taken)
    else
      output = GMQL_server.setOutputPath("").TAKE(taken, how_many)

    mem_regions = output.asInstanceOf[GMQL_DATASET]._1.
      map(x => Array[String](x._1._1.toString, x._1._2, x._1._3.toString, x._1._4.toString, x._1._5.toString) ++ x._2.
        map(s => s.toString))

    mem_meta = output.asInstanceOf[GMQL_DATASET]._2.
      map(x => Array[String](x._1.toString, x._2._1, x._2._2))

    mem_schema = output.asInstanceOf[GMQL_DATASET]._3.
      map(x => Array[String](x._1)).toArray

    //vv = vv.empty
    change_processing_possible = true
    Array("0","Executed")
  }

  def get_reg(): Array[Array[String]] = {
    mem_regions
  }

  def get_meta(): Array[Array[String]] = {
    mem_meta
  }

  def get_schema(): Array[Array[String]] = {
    mem_schema
  }

  def create_list_schema(schema: Array[Array[String]]): List[(String, PARSING_TYPE)] = {

    var temp = ListBuffer[(String, PARSING_TYPE)]()
    for (elem <- schema) {
      temp += ((elem(1), getParsingTypeFromString(elem(0))))
    }
    temp.toList
  }


  /*GMQL OPERATION*/

  def select(predicate: String, region_predicate: String, semi_join: Array[Array[String]],
             input_dataset: String): Array[String] = {

    if (vv.get(input_dataset).isEmpty)
      return Array("1","No valid Data as input")

    val dataAsTheyAre = vv(input_dataset)

    //println(region_predicate)
    //println(predicate)

    var semiJoinDataAsTheyAre: IRVariable = null
    var semi_join_metaDag: Option[MetaOperator] = None
    var semi_join_list:Option[MetaJoinCondition]= None

    var selected_meta: (String, Option[MetadataCondition]) = ("", None)
    var selected_regions: (String, Option[RegionCondition]) = ("", None)
    val parser = new Parser(dataAsTheyAre, GMQL_server)

    if (predicate != null) {
      val str_predicate = parser.findAndChangeMeta(predicate)
      selected_meta = parser.parseSelectMetadata(str_predicate)
      if (selected_meta._2.isEmpty)
        return Array("1",selected_meta._1)
    }

    if (region_predicate != null) {
      val str_predicate = parser.findAndChangeReg(region_predicate)
      selected_regions = parser.parseSelectRegions(str_predicate)
      if (selected_regions._2.isEmpty)
        return Array("1",selected_regions._1)
    }

    if(semi_join!=null)
    {
      val semijoin_data  = semi_join(0)(0)
      if (vv.get(semijoin_data).isEmpty)
        return Array("1","No valid Data as semijoin")

      semiJoinDataAsTheyAre = vv(semijoin_data)
      val is_in = semi_join(0)(1)
      val semi_join_neg = is_in match{
        case "TRUE" => true
        case "FALSE" => false
      }
      val semi_join_cond = semi_join.drop(1)
      semi_join_list= MetaJoinConditionList_with_neg(semi_join_cond,!semi_join_neg)
      if (semi_join_list.isEmpty)
        return Array("1","No valid condition in semijoin")

      semi_join_metaDag = Some(semiJoinDataAsTheyAre.metaDag)
    }

    val select = dataAsTheyAre.add_select_statement(semi_join_metaDag, semi_join_list, selected_meta._2, selected_regions._2)

    val index = counter.getAndIncrement()
    val out_p = input_dataset + "/select" + index
    vv = vv + (out_p -> select)

    Array("0",out_p)

  }

  def project(projected_meta: Array[String], extended_meta: String, all_but_meta: Boolean,
              projected_region: Array[String], extended_values: String, all_but_reg:Boolean,
              input_dataset: String): Array[String] = {

    if (vv.get(input_dataset).isEmpty)
      return Array("1","No valid Data as input")

    var regions_index: (String, Option[List[Int]]) = ("", None)
    var regions_in_but: (String, Option[List[String]]) = ("", None)
    var extended_reg: (String, Option[List[RegionFunction]]) = ("", None)
    var extended_m: (String, Option[List[MetaExtension]]) = ("", None)
    val dataAsTheyAre = vv(input_dataset)
    val parser = new Parser(dataAsTheyAre, GMQL_server)

    //println(extended_meta)
    //println(extended_values)

    val meta_list: Option[List[String]] = MetadataAttributesList(projected_meta)

    if (projected_region != null)
    {
      if (!all_but_reg) {
        regions_index = regionsList(projected_region, dataAsTheyAre)
        if (regions_index._2.isEmpty)
          return Array("1",regions_index._1)
      }
      else {
        regions_in_but = regionsList_but(projected_region, dataAsTheyAre)
        if (regions_in_but._2.isEmpty)
          return Array("1",regions_in_but._1)
      }
    }

    if (extended_values != null) {
      extended_reg = parser.parseProjectRegion(extended_values.toString)
      if (extended_reg._2.isEmpty)
        return Array("1",extended_reg._1)
    }

    if(extended_meta != null) {
      extended_m = parser.parseProjectMetdata(extended_meta.toString)
      if (extended_m._2.isEmpty)
        return Array("1",extended_m._1)
    }

    val project = dataAsTheyAre.PROJECT(meta_list, extended_m._2, all_but_meta, regions_index._2,
      regions_in_but._2, extended_reg._2)
    val index = counter.getAndIncrement()

    val out_p = input_dataset + "/project" + index
    vv = vv + (out_p -> project)

    Array("0",out_p)
  }

  def extend(metadata: Array[Array[String]], input_dataset: String): Array[String] = {
    if (vv.get(input_dataset).isEmpty)
      return Array("1","No valid Data as input")

    val dataAsTheyAre = vv(input_dataset)
    var meta_list: (String, List[RegionsToMeta]) = ("", List())

    if (metadata != null) {
      meta_list = RegionsToMetaFactory(metadata, dataAsTheyAre)
      if (meta_list._2.isEmpty)
        return Array("1",meta_list._1)
    }

    val extend = dataAsTheyAre.EXTEND(meta_list._2)

    val index = counter.getAndIncrement()
    val out_p = input_dataset + "/extend" + index
    vv = vv + (out_p -> extend)

    Array("0",out_p)
  }

  def merge(group_by:Array[Array[String]], input_dataset: String): Array[String] = {
    if (vv.get(input_dataset).isEmpty)
      return Array("1","No valid Data as input")

    val dataAsTheyAre = vv(input_dataset)

    val meta_condition_list: Option[List[AttributeEvaluationStrategy]] = MetaAttributeEvaluationStrategyList(group_by)
    val merge = dataAsTheyAre.MERGE(meta_condition_list)

    val index = counter.getAndIncrement()
    val out_p = input_dataset + "/merge" + index
    vv = vv + (out_p -> merge)

    Array("0",out_p)
  }

  def group(meta_keys:Array[Array[String]], meta_aggr:Array[Array[String]], region_keys:Array[String],
            aggregates:Array[Array[String]], input_dataset: String): Array[String] = {

    if (vv.get(input_dataset).isEmpty)
      return Array("1","No valid Data as input")

    var aggr_list: (String, List[RegionsToRegion]) = ("", List())
    var aggr_list_opt: Option[List[RegionsToRegion]] = None
    var meta_aggr_list: (String, Option[List[MetaAggregateFunction]]) = ("", None)
    var group_reg_list:(String, Option[List[GroupingParameter]]) = ("", None)
    val dataAsTheyAre = vv(input_dataset)
    var metaGroupByList:Option[MetaGroupByCondition] = None

    if(meta_keys!=null) {
      val groupList: Option[List[AttributeEvaluationStrategy]] = MetaAttributeEvaluationStrategyList(meta_keys)
      if (groupList.isDefined)
        metaGroupByList = Some(MetaGroupByCondition(groupList.get))
    }

    if (aggregates != null) {
      aggr_list = RegionToRegionAggregates(aggregates, dataAsTheyAre)
      if (aggr_list._2.isEmpty)
        return Array("1",aggr_list._1)

      aggr_list_opt = Some(aggr_list._2)
    }

    if(region_keys !=null ) {
      group_reg_list = groupingParam(region_keys, dataAsTheyAre)
      if (group_reg_list._2.isEmpty)
        return Array("1", group_reg_list._1)
    }

    if(meta_aggr !=null ) {
      meta_aggr_list = MetaAggregates(meta_aggr, dataAsTheyAre)
      if (meta_aggr_list._2.isEmpty)
        return Array("1", meta_aggr_list._1)
    }

    val group = dataAsTheyAre.GROUP(metaGroupByList,meta_aggr_list._2,"_group",group_reg_list._2,aggr_list_opt)

    val index = counter.getAndIncrement()
    val out_p = input_dataset + "/group" + index
    vv = vv + (out_p -> group)

    Array("0",out_p)
  }



  def order(meta_order: Array[Array[String]], meta_opt:String, meta_opt_value:Int, reg_opt:String, reg_opt_value:Int,
            region_order: Array[Array[String]], input_dataset: String): Array[String] = {

    if (vv.get(input_dataset).isEmpty)
      return Array("1","No valid Data as input")

    val dataAsTheyAre = vv(input_dataset)

    val m_top: TopParameter = meta_opt match{
      case "mtop" =>  Top(meta_opt_value)
      case "mtopp" => TopP(meta_opt_value)
      case "mtopg" => TopG(meta_opt_value)
      case _ => NoTop()
    }

    val r_top: TopParameter = reg_opt match{
      case "rtop" =>  Top(reg_opt_value)
      case "rtopp" => TopP(reg_opt_value)
      case "rtopg" => TopG(reg_opt_value)
      case _ => NoTop()
    }

    val meta_list = meta_order_list(meta_order)

    var reg_ordering: (String, Option[List[(Int, Direction)]]) = ("", None)
    if (region_order != null) {
      reg_ordering = region_order_list(region_order, dataAsTheyAre)
      if (reg_ordering._2.isEmpty)
        return Array("1",reg_ordering._1)
    }
    val order = dataAsTheyAre.ORDER(meta_list, "_order", m_top, reg_ordering._2, r_top)

    val index = counter.getAndIncrement()
    val out_p = input_dataset + "/order" + index
    vv = vv + (out_p -> order)

    Array("0",out_p)
  }

  //we use "right" and "left" as prefixes
  def union(left_dataset: String, right_dataset: String): Array[String] = {
    if (vv.get(right_dataset).isEmpty)
      return Array("1","No valid right Data as input")

    if (vv.get(left_dataset).isEmpty)
      return Array("1","No valid left Data as input")

    val leftDataAsTheyAre = vv(left_dataset)
    val rightDataAsTheyAre = vv(right_dataset)

    val union = leftDataAsTheyAre.UNION(rightDataAsTheyAre, "left", "right")

    val index = counter.getAndIncrement()
    val out_p = left_dataset + "/union" + index
    //val out_p = left_dataset+right_dataset+"/union"+index
    vv = vv + (out_p -> union)

    Array("0",out_p)
  }

  def difference(join_by: Array[Array[String]], left_dataset: String,
                 right_dataset: String, is_exact: Boolean): Array[String] = {

    if (vv.get(right_dataset).isEmpty)
      return Array("1","No valid right Data as input")

    if (vv.get(left_dataset).isEmpty)
      return Array("1","No valid left Data as input")

    val leftDataAsTheyAre = vv(left_dataset)
    val rightDataAsTheyAre = vv(right_dataset)

    val meta_condition_list: Option[MetaJoinCondition] = MetaJoinConditionList(join_by)

    val difference = leftDataAsTheyAre.DIFFERENCE(meta_condition_list, rightDataAsTheyAre, is_exact)

    val index = counter.getAndIncrement()
    //val out_p = left_dataset+right_dataset+"/difference"+index
    val out_p = left_dataset + "/difference" + index
    vv = vv + (out_p -> difference)

    Array("0",out_p)
  }

  /* COVER, FLAT, SUMMIT, HISTOGRAM */

  def flat(min: String, max: String, groupBy: Array[Array[String]], aggregates: Array[Array[String]],
           input_dataset: String): Array[String] = {
    //println("flat")
    val (error, flat) = doVariant(CoverFlag.FLAT, min, max, groupBy, aggregates, input_dataset)
    if (flat == null)
      return Array("1",error)

    val index = counter.getAndIncrement()
    val out_p = input_dataset + "/flat" + index
    vv = vv + (out_p -> flat)

    Array("0",out_p)
  }

  def histogram(min: String, max: String, groupBy: Array[Array[String]], aggregates: Array[Array[String]],
                input_dataset: String): Array[String] = {
    val (error, histogram) = doVariant(CoverFlag.HISTOGRAM, min, max, groupBy, aggregates, input_dataset)
    //println("histogram")

    if (histogram == null)
      return Array("1",error)

    val index = counter.getAndIncrement()
    val out_p = input_dataset + "/histogram" + index
    vv = vv + (out_p -> histogram)

    Array("0",out_p)
  }

  def summit(min: String, max: String, groupBy: Array[Array[String]], aggregates: Array[Array[String]],
             input_dataset: String): Array[String] = {
    //println("summit")
    val (error, summit) = doVariant(CoverFlag.SUMMIT, min, max, groupBy, aggregates, input_dataset)
    if (summit == null)
      return Array("1",error)

    val index = counter.getAndIncrement()
    val out_p = input_dataset + "/summit" + index
    vv = vv + (out_p -> summit)

    Array("0",out_p)
  }

  def cover(min: String, max: String, groupBy: Array[Array[String]], aggregates: Array[Array[String]],
            input_dataset: String): Array[String] = {
    //println("cover")

    val (error, cover) = doVariant(CoverFlag.COVER, min, max, groupBy, aggregates, input_dataset)
    if (cover == null)
      return Array("1",error)

    val index = counter.getAndIncrement()
    val out_p = input_dataset + "/cover" + index
    vv = vv + (out_p -> cover)

    Array("0",out_p)
  }


  def doVariant(flag: CoverFlag.CoverFlag, min: String, max: String, groupBy: Array[Array[String]],
                aggregates: Array[Array[String]], input_dataset: String): (String, IRVariable) = {
    if (vv.get(input_dataset).isEmpty)
      return ("No valid dataset as input", null)

    val dataAsTheyAre = vv(input_dataset)
    var aggr_list: (String, List[RegionsToRegion]) = ("", List())

    val parser = new Parser()

    val pMin = parser.findAndChangeCover(min)
    if(pMin._1 =="Invalid Syntax" || pMin._1 =="Failure")
      return (pMin._1,null)

    val CoverMin = CoverParameterManager.getCoverParam(pMin._1,pMin._2,pMin._3)

    val pMax = parser.findAndChangeCover(max)
    if(pMax._1 =="Invalid Syntax" || pMax._1 =="Failure")
      return (pMax._1,null)

    val CoverMax = CoverParameterManager.getCoverParam(pMax._1,pMax._2,pMax._3)

    if (aggregates != null) {
      aggr_list = RegionToRegionAggregates(aggregates, dataAsTheyAre)
      if (aggr_list._2.isEmpty)
        return (aggr_list._1, null)
    }

    val groupList: Option[List[AttributeEvaluationStrategy]] = MetaAttributeEvaluationStrategyList(groupBy)

    val variant = dataAsTheyAre.COVER(flag, CoverMin, CoverMax, aggr_list._2, groupList)

    ("OK", variant)
  }


  // we do not add left, right: we set to None
  def map(condition: Array[Array[String]], aggregates: Array[Array[String]],
          count_name:String, left_dataset: String, right_dataset: String): Array[String] = {
    if (vv.get(right_dataset).isEmpty)
      return Array("1","No valid right Data as input")

    if (vv.get(left_dataset).isEmpty)
      return Array("1","No valid left Data as input")

    val leftDataAsTheyAre = vv(left_dataset)
    val rightDataAsTheyAre = vv(right_dataset)

    var aggr_list: (String, List[RegionsToRegion]) = ("", List())

    if (aggregates != null) {
      aggr_list = RegionToRegionAggregates(aggregates, leftDataAsTheyAre)
      if (aggr_list._2.isEmpty)
        return Array("1",aggr_list._1)
    }
    val condition_list: Option[MetaJoinCondition] = MetaJoinConditionList(condition)

    var c_name = Option("")
    if(count_name!=null)
      c_name = Option(count_name)
    else
      c_name = None

    val map = leftDataAsTheyAre.MAP(condition_list, aggr_list._2, rightDataAsTheyAre, None,None, c_name)

    val index = counter.getAndIncrement()
    val out_p = left_dataset + "/map" + index
    vv = vv + (out_p -> map)

    Array("0",out_p)
  }


  // we do not add ref and exp name: we set to None
  def join(genometric: Array[Array[String]], meta_join: Array[Array[String]], output: String,
           attributes: Array[String], left_dataset: String, right_dataset: String): Array[String] = {

    if(GMQL_server == null)
      return Array("1","invoke init_gmql() first")

    if (vv.get(right_dataset).isEmpty)
      return Array("1","No valid right Data as input")

    if (vv.get(left_dataset).isEmpty)
      return Array("1","No valid left Data as input")

    val leftDataAsTheyAre = vv(left_dataset)
    val rightDataAsTheyAre = vv(right_dataset)
    var attr_list: (String, Option[List[(Int, Int)]]) = ("",None)

    if (attributes != null) {
      attr_list = joinRegAttributeList(attributes, leftDataAsTheyAre)
      if (attr_list._2.isEmpty)
        return Array("1",attr_list._1)
    }
    val meta_join_list: Option[MetaJoinCondition] = MetaJoinConditionList(meta_join)
    val region_join_list: List[JoinQuadruple] = RegionQuadrupleList(genometric)

    val reg_out = regionBuild(output)

    val join = leftDataAsTheyAre.JOIN(meta_join_list, region_join_list, reg_out,
      rightDataAsTheyAre, None, None, attr_list._2)

    val index = counter.getAndIncrement()
    // val out_p = left_dataset+right_dataset+"/join"+index
    val out_p = left_dataset + "/join" + index
    vv = vv + (out_p -> join)

    Array("0",out_p)
  }



  def regionBuild(output: String): RegionBuilder = {

    output match {
      case "LEFT" => RegionBuilder.LEFT
      case "RIGHT" => RegionBuilder.RIGHT
      case "CAT" => RegionBuilder.CONTIG
      case "INT" => RegionBuilder.INTERSECTION
      case "BOTH" => RegionBuilder.BOTH
      case "LEFT_DIST" => RegionBuilder.LEFT_DISTINCT
      case "RIGHT_DIST" => RegionBuilder.RIGHT_DISTINCT
    }

  }


  def groupingParam(params: Array[String],data: IRVariable): (String, Option[List[GroupingParameter]]) =
  {
    var group_List: Option[List[GroupingParameter]] = None
    var temp_list = new ListBuffer[GroupingParameter]()
    for (elem <- params) {
      val field = data.get_field_by_name(elem)
      if (field.isEmpty) {
        val error = "No value " + elem(2) + " from this schema"
        return (error, group_List) //empty list
      }
      var pos = field.get
      temp_list += FIELD(pos)
    }
    group_List = Some(temp_list.toList)

    ("OK",group_List)
  }


  def RegionsToMetaFactory(aggregates: Any, data: IRVariable): (String, List[RegionsToMeta]) = {
    var region_meta_list: List[RegionsToMeta] = List()
    var temp_list = new ListBuffer[RegionsToMeta]()

    aggregates match {
      case aggregates: Array[Array[String]] => {

        for (elem <- aggregates) {
          if (elem(1).equalsIgnoreCase("COUNT")) {
            var res = DefaultRegionsToMetaFactory.get(elem(1), Some(elem(0)))
            res.output_attribute_name = elem(0)
            temp_list += res
          }
          else {
            val field = data.get_field_by_name(elem(2))
            if (field.isEmpty) {
              val error = "No value " + elem(2) + " from this schema"
              return (error, region_meta_list) //empty list
            }
            var res = DefaultRegionsToMetaFactory.get(elem(1), field.get, Some(elem(0)))
            res.output_attribute_name = elem(0)
            temp_list += res
          }
        }
        region_meta_list = temp_list.toList
      }
    }
    ("OK", region_meta_list) // not empty list
  }

  def RegionToRegionAggregates(aggregates: Any, data: IRVariable): (String, List[RegionsToRegion]) = {
    var aggr_list: List[RegionsToRegion] = List()
    val temp_list = new ListBuffer[RegionsToRegion]()

    aggregates match {
      case aggregates: Array[Array[String]] => {

        for (elem <- aggregates) {
          if (elem(1).equalsIgnoreCase("COUNT"))
          {
            var res = DefaultRegionsToRegionFactory.get(elem(1), Some(elem(0)))
            res.output_name = Some(elem(0))
            temp_list += res
          }
          else {
            val field = data.get_field_by_name(elem(2))
            if (field.isEmpty) {
              val error = "No value " + elem(2) + " from this schema"
              return (error, aggr_list) //empty list
            }
            var res = DefaultRegionsToRegionFactory.get(elem(1), field.get, Some(elem(0)))
            res.output_name = Some(elem(0))
            temp_list += res
          }
        }
        aggr_list = temp_list.toList
      }
    }
    ("OK", aggr_list) // not empty list
  }

  def MetaAggregates(aggregates: Any, data: IRVariable): (String, Option[List[MetaAggregateFunction]]) = {
    var meta_aggr_list: List[MetaAggregateFunction] = List()
    val temp_list = new ListBuffer[MetaAggregateFunction]()

    aggregates match
    {
      case aggregates: Array[Array[String]] => {

        for (elem <- aggregates) {
          try {
            if (elem(1).equalsIgnoreCase("COUNTSAMP")) {
              var res = DefaultMetaAggregateFactory.get(elem(1), Some(elem(0)))
              //res.output_name = Some(elem(0))
              temp_list += res
            }
            else {
              var res = DefaultMetaAggregateFactory.get(elem(1), elem(2), Some(elem(0)))
              //res.output_name = Some(elem(0))
              temp_list += res
            }
          }
          catch {
            case fe: Exception => return (fe.getMessage, None)
          }
        }
      }
        meta_aggr_list = temp_list.toList
    }

    ("OK", Some(meta_aggr_list)) // not empty list
  }



  def MetadataAttributesList(group_by: Array[String]): Option[List[String]] = {
    var group_list: Option[List[String]] = None
    val temp_list = new ListBuffer[String]()

    if(group_by==null) {
     // println("null")
      return group_list
    }
    //println("not null")
    for (elem <- group_by)
      temp_list += elem
    if (temp_list.nonEmpty)
       group_list = Some(temp_list.toList)

    group_list

  }

  def MetaJoinConditionList_with_neg(join_by: Array[Array[String]],neg:Boolean): Option[MetaJoinCondition] = {
    var join_by_list: Option[MetaJoinCondition] = None
    val joinList = new ListBuffer[AttributeEvaluationStrategy]()

    if (join_by == null  )
      return join_by_list

    for (elem <- join_by) {
      val attribute = elem(0)
      attribute match {
        case "DEF" => joinList += Default(elem(1))
        case "FULL" => joinList += FullName(elem(1))
        case "EXACT" => joinList += Exact(elem(1))
      }
    }
    if (joinList.nonEmpty)
      join_by_list = Some(MetaJoinCondition(joinList.toList,neg))

    join_by_list
  }

  def MetaJoinConditionList(join_by: Array[Array[String]]): Option[MetaJoinCondition] = {
    var join_by_list: Option[MetaJoinCondition] = None
    val joinList = new ListBuffer[AttributeEvaluationStrategy]()

    if (join_by == null  ) {
      return join_by_list
    }

    for (elem <- join_by) {
      val attribute = elem(0)
     // print(elem(0), elem(1))

      attribute match {
        case "DEF" => joinList += Default(elem(1))
        case "FULL" => joinList += FullName(elem(1))
        case "EXACT" => joinList += Exact(elem(1))
      }
    }
    if (joinList.nonEmpty)
      join_by_list = Some(MetaJoinCondition(joinList.toList))

    join_by_list
  }

  def MetaAttributeEvaluationStrategyList(join_by: Array[Array[String]]): Option[List[AttributeEvaluationStrategy]] = {
    var join_by_list: Option[List[AttributeEvaluationStrategy]] = None
    val joinList = new ListBuffer[AttributeEvaluationStrategy]()

    if (join_by == null  ) {
      return join_by_list
    }

    for (elem <- join_by) {
      val attribute = elem(0)
      attribute match {
        case "DEF" => joinList += Default(elem(1))
        case "FULL" => joinList += FullName(elem(1))
        case "EXACT" => joinList += Exact(elem(1))
      }
    }
    if (joinList.nonEmpty)
      join_by_list = Some(joinList.toList)

    join_by_list
  }

  def regionsList_but(projected_by: Array[String], data: IRVariable): (String, Option[List[String]]) = {
    var projectedList: Option[List[String]] = None
    val temp_list = new ListBuffer[String]()

    projected_by match {
      case projected_by: Array[String] => {
        for (elem <- projected_by) {
          val field = data.get_field_by_name(elem)
          if (field.isEmpty) {
            val error = "No value " + elem + " from this schema"
            return (error, projectedList) //empty list
          }
          temp_list += elem
        }
      }
    }
    projectedList = Some(temp_list.toList)
    ("OK", projectedList)
  }


  def regionsList(projected_by: Array[String], data: IRVariable): (String, Option[List[Int]]) = {
    var projectedList: Option[List[Int]] = None
    val temp_list = new ListBuffer[Int]()

    projected_by match {
      case projected_by: Array[String] => {
        for (elem <- projected_by) {
          val field = data.get_field_by_name(elem)
          if (field.isEmpty) {
            val error = "No value " + elem + " from this schema"
            return (error, projectedList) //empty list
          }
          temp_list += field.get
        }
      }
    }
    projectedList = Some(temp_list.toList)
    ("OK", projectedList)
  }


  def meta_order_list(order_matrix: Any): Option[List[(String, Direction)]] = {

    var order_list: Option[List[(String, Direction)]] = None
    val temp_list = new ListBuffer[(String, Direction)]()

    if (order_matrix == null)
      return order_list

    order_matrix match {
      case order_matrix: Array[Array[String]] => {
        for (elem <- order_matrix) {
          var dir = Direction.ASC
          if (elem(0) == "DESC")
            dir = Direction.DESC

          temp_list += ((elem(1), dir))
        }
      }
    }
    order_list = Some(temp_list.toList)

    order_list
  }

  def region_order_list(order_matrix: Any, data: IRVariable): (String, Option[List[(Int, Direction)]]) = {

    var order_list: Option[List[(Int, Direction)]] = None
    val temp_list = new ListBuffer[(Int, Direction)]()

    order_matrix match {
      case order_matrix: Array[Array[String]] => {
        for (elem <- order_matrix) {
          val field = data.get_field_by_name(elem(1))
          if (field.isEmpty) {
            val error = "No value " + elem(1) + " from this schema"
            return (error, order_list) //empty list
          }
          if (elem(0) == "ASC") {
            val dir = Direction.ASC
            temp_list += ((field.get, dir))
          }
          else {
            val dir = Direction.DESC
            temp_list += ((field.get, dir))
          }
        }
      }
    }
    order_list = Some(temp_list.toList)

    ("OK", order_list)
  }


  def RegionQuadrupleList(quad_join: Array[Array[String]]): List[JoinQuadruple] = {

    var quad_list: List[JoinQuadruple] = null
    val temp_list = new ListBuffer[Option[AtomicCondition]]()

    if (quad_join == null) {
      return quad_list
    }

    val len = quad_join.length

    for (elem <- quad_join) {
      //print(elem(0), elem(1))
      temp_list += atomic_cond(elem(0), elem(1))
    }
    if(len < 4)
      for(i <- 0 until 4-len)
        temp_list += None

    val quadruple = JoinQuadruple(temp_list.head,temp_list(1),temp_list(2),temp_list(3))
    List(quadruple)

  }

  def joinRegAttributeList(reg_attributes: Array[String], data: IRVariable): (String, Option[List[(Int, Int)]]) = {
    var attributeList: Option[List[(Int, Int)]] = None
    val temp_list = new ListBuffer[(Int, Int)]()

    for (elem <- reg_attributes) {
      val field = data.get_field_by_name(elem)
      if (field.isEmpty) {
        val error = "No value " + elem + " from this schema"
        return (error, attributeList) //empty list
      }
      val parse_type = data.get_type_by_name(elem)

      temp_list += ((field.get, parse_type.get.##))
    }
    attributeList = Some(temp_list.toList)
    ("OK", attributeList)
  }



  def atomic_cond(cond: String, value: String): Option[AtomicCondition] = {
    cond match {
      case "DGE" => Some(DistGreater(value.toLong - 1))
      case "DLE" => Some(DistLess(value.toLong + 1))
      case "DG" => Some(DistGreater(value.toLong))
      case "DL" => Some(DistLess(value.toLong))
      case "UP" => Some(Upstream())
      case "DOWN" => Some(DownStream())
      case "MD" => Some(MinDistance(value.toInt))
      case _ => None
    }
  }

  def save_tokenAndUrl(token:String,url:String): Unit =
  {
    rest_manager.save_tokenAndUrl(token,url )
  }

  def delete_token(): Unit =
  {
    rest_manager.delete_token()
  }

  def get_dataset_list: Array[Array[String]] =
  {
    this.datasetQueue.toArray
  }

  def get_url: String =
  {
    rest_manager.service_url
  }

  def main(args: Array[String]): Unit =
  {
    /*
    initGMQL("GTF",true)
    rest_manager.service_token = "14ae473f-4c08-4da5-9339-606c7845056a"
    rest_manager.service_url = "http://genomic.deib.polimi.it/gmql-rest-test/"
    val dataset1 = "/Users/simone/Desktop/datasets/dataset_1/files"
    val dataset2 = "public.Example_Dataset_1"
    val dataset1_schema = dataset1 + "/schema.schema"

    val schema = Array(
      Array("CHR",	"STRING"),
      Array("start","LONG"),
      Array("stop","LONG"),
      Array("name",	"STRING"),
      Array("score","DOUBLE"),
      Array("strand","STRING"),
      Array("signal","DOUBLE"),
      Array("pvalue","DOUBLE"),
      Array("qvalue","DOUBLE"))


    val schemagtf = Array(
      Array("seqname",	"STRING"),
      Array("source","STRING"),
      Array("feature","STRING"),
      Array("start",	"LONG"),
      Array("end","LONG"),
      Array("score","DOUBLE"),
    Array("strand","STRING"),
      Array("frame","STRING"),
      Array("name","STRING"),
    Array("signal","DOUBLE"),
      Array("pvalue","DOUBLE"),
    Array("qvalue","DOUBLE"),
    Array("peak","DOUBLE"))

    val DS1 = readDataset(dataset2,"CUSTOMPARSER",false,true,schemagtf,null,"default","GTF")

    //val predicate = "patient_age < 70"
    //val s = select(predicate,null,null,DS1(1))
    //materialize(DS1(1),"pred")
    //execute()

    val groupBy = Array(Array("DEF","biosample_term_name"),
      Array("DEF","experiment_target"))

    val s1 = cover("1","ALL",null,null,DS1(1))

    materialize(s1(1),"cover")
    val b = execute()
*/
  }

}
