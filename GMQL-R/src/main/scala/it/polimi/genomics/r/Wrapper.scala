package it.polimi.genomics.r

import java.io.FileNotFoundException
import java.util.concurrent.atomic.AtomicLong

import it.polimi.genomics.GMQLServer.{DefaultRegionsToMetaFactory, DefaultRegionsToRegionFactory, GmqlServer}
import it.polimi.genomics.core.DataStructures.CoverParameters._
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor.GMQL_DATASET
import it.polimi.genomics.core.DataStructures.GroupMDParameters.Direction.Direction
import it.polimi.genomics.core.DataStructures.GroupMDParameters._
import it.polimi.genomics.core.DataStructures.JoinParametersRD._
import it.polimi.genomics.core.DataStructures.JoinParametersRD.RegionBuilder.RegionBuilder
import it.polimi.genomics.core.DataStructures.MetaAggregate.MetaExtension
import it.polimi.genomics.core.DataStructures.{IRVariable, MetaOperator}
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

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Created by simone on 21/03/17.
  */


object Wrapper {
  var GMQL_server: GmqlServer = _
  var Spark_context: SparkContext = _

  var remote_processing: Boolean = false
  //var data_to_send_to_repo: ListBuffer[String] = _
  var outputformat: GMQLSchemaFormat.Value = _
  var executing_query: Boolean = true
  //thread safe counter for unique string pointer to dataset
  //'cause we could have two pointer at the same dataset and we
  //can not distinguish them

  //example:
  // R shell
  //r = readDataset("/Users/simone/Downloads/job_filename_guest_new14_20170316_162715_DATA_SET_VAR")
  //r1 = readDataset("/Users/simone/Downloads/job_filename_guest_new14_20170316_162715_DATA_SET_VAR")
  //are mapped in vv with the same key
  var counter: AtomicLong = new AtomicLong(0)

  var materialize_count: AtomicLong = new AtomicLong(0)

  var mem_meta: Array[Array[String]] = _
  var mem_regions: Array[Array[String]] = _
  var mem_schema: Array[Array[String]] = _

  var vv: Map[String, IRVariable] = Map[String, IRVariable]()


  def getGvalueArray(value: Array[String]): Array[GValue] = {
    val temp: ArrayBuffer[GValue] = new ArrayBuffer[GValue]()
    for (elem <- value) {
      if (elem.equalsIgnoreCase("."))
        temp += GString(".")

      else if (elem matches "[\\+\\-0-9.e]+")
        temp += GDouble(elem.toDouble)
      else {
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

    if (GMQL_server == null) {
      println("GMQL Server is down")
      return
    }

    remote_processing = remote_proc
    println("GMQL Server is up")
  }

  def remote_processing(remote: Boolean): String = {
    if( executing_query) {
      remote_processing = remote

      if(!remote_processing)
        "Remote processing off"
      else
        "Remote processing On"
    }
    else
      "You cannot modify processing mode"
  }

  def is_remote_processing(): Boolean = {
    remote_processing
  }

  def outputFormat(format: String): GMQLSchemaFormat.Value = {
    format match {
      case "TAB" => GMQLSchemaFormat.TAB
      case "GTF" => GMQLSchemaFormat.GTF
      //case "VCF" => GMQLSchemaFormat.VCF
      case "COLLECT" => GMQLSchemaFormat.COLLECT
    }
  }

  def readDataset(data_input_path: String, parser_name: String, is_local: Boolean,
                  schema: Array[Array[String]]): String =
  {
    var parser: BedParser = null
    var out_p = ""
    var data_path = data_input_path
    if (is_local)
      data_path = data_path + "/files"

    parser_name match {
      case "BEDPARSER" => parser = BedParser
      case "ANNPARSER" => parser = ANNParser
      case "BROADPROJPARSER" => parser = BroadProjParser
      case "BASICPARSER" => parser = BasicParser
      case "NARROWPEAKPARSER" => parser = NarrowPeakParser
      case "RNASEQPARSER" => parser = RnaSeqParser
      case "CUSTOMPARSER" => {
        if (is_local) {
          parser = new CustomParser()
          try {
            parser.asInstanceOf[CustomParser].setSchema(data_path)
          }
          catch {
            case fe: FileNotFoundException => return fe.getMessage
          }
        }
        else {
          if (schema != null)
            parser = createParser(schema)
          else
            return "No schema defined"
        }
      }
      case _ => return "No parser defined"
    }

    executing_query = false
    val dataAsTheyAre = GMQL_server.READ(data_path).USING(parser)
    val index = counter.getAndIncrement()
    out_p = "dataset" + index
    vv = vv + (out_p -> dataAsTheyAre)

    out_p

  }

  def createParser(schema: Array[Array[String]]): BedParser = {

    var chr_index = 0
    var start_index = 0
    var end_index = 0
    var strand_index = 0
    val schemaList = new ListBuffer[(Int, ParsingType.PARSING_TYPE)]()

    for (i <- 0 until schema.length) {
      var name = schema(i)(0)
      var parse_type = schema(i)(1)
      name match {
        case "seqnames" | "chr" => chr_index = i
        case "start" | "lefg" => start_index = i
        case "end" | "right" => end_index = i
        case "strand" => strand_index = i
        case _ => {
          var pt = getParsingTypeFromString(parse_type)
          schemaList += ((i, pt))
        }
      }
    }
    var parser = new BedParser("\t", chr_index, start_index, end_index, Some(strand_index), Some(schemaList.toArray))
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
      case _ => ParsingType.DOUBLE
    }
  }

  def read(meta: Array[Array[String]], regions: Array[Array[String]], schema: Array[Array[String]]): String = {

    val metaDS = Spark_context.parallelize(meta.map { x => (x(0).toLong, (x(1), x(2))) })
    val regionDS = Spark_context.parallelize(regions.map {
      x => (new GRecordKey(x(0).toLong, x(1), x(2).toLong, x(3).toLong, x(4).toCharArray.head), getGvalueArray(x.drop(4)))
    })

    val schemaDS = create_list_schema(schema)

    val dataAsTheyAre = GMQL_server.READ("").USING(metaDS, regionDS, schemaDS)

    val index = counter.getAndIncrement()
    val out_p = "dataset" + index
    vv = vv + (out_p -> dataAsTheyAre)

    out_p
  }


  def materialize(data_to_materialize: String, data_output_path: String): String = {

    if (vv.get(data_to_materialize).isEmpty)
      return "No valid Data to materialize"

    val materialize = vv(data_to_materialize)
    GMQL_server setOutputPath data_output_path MATERIALIZE materialize
    materialize_count.getAndIncrement()

    "Materialized"
  }

  def execute(): String =
  {
    if(outputformat == GMQLSchemaFormat.COLLECT)
      return "No execute() available, you choose memory as output," +
        " use take() function instead"

    if (materialize_count.get() <= 0)
      "You must materialize before"
    else {
      GMQL_server.run()
      materialize_count.set(0)
      executing_query = true
      vv = vv.empty
      "Executed"
    }
  }

  def take(data_to_take: String, how_many: Int): String = {
    if (vv.get(data_to_take).isEmpty)
      return "No valid Data to take"

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

    executing_query = true
    vv = vv.empty
    "Executed"
  }

  def get_reg(): Array[Array[String]] = {
    if (mem_regions == null)
      return Array(Array("NO regions in memory"))
    mem_regions
  }

  def get_meta(): Array[Array[String]] = {
    if (mem_meta == null)
      return Array(Array("NO metadata in memory"))
    mem_meta
  }

  def get_schema(): Array[Array[String]] = {
    if (mem_schema == null)
      return Array(Array("NO schema in memory"))
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
             semi_join_dataset: String, semi_join_neg:Boolean, input_dataset: String): String = {

    if (vv.get(input_dataset).isEmpty)
      return "No valid Data as input"

    val dataAsTheyAre = vv(input_dataset)

    var semiJoinDataAsTheyAre: IRVariable = null
    var semi_join_metaDag: Option[MetaOperator] = None
    var selected_meta: (String, Option[MetadataCondition]) = ("", None)
    var selected_regions: (String, Option[RegionCondition]) = ("", None)
    val parser = new Parser(dataAsTheyAre, GMQL_server)
    var semi_join_list:Option[MetaJoinCondition]=None

    if (predicate != null) {
      selected_meta = parser.parseSelectMetadata(predicate.toString)
      if (selected_meta._2.isEmpty)
        return selected_meta._1
    }

    if (region_predicate != null) {
      selected_regions = parser.parseSelectRegions(region_predicate.toString)
      if (selected_regions._2.isEmpty)
        return selected_regions._1
    }

    if(semi_join!=null){
      semi_join_list= MetaJoinConditionList_with_neg(semi_join,semi_join_neg)
      if (semi_join_list.isEmpty)
        return "No valid condition in semi join"

      if (semi_join_list.isDefined) {
        if (vv.get(semi_join_dataset.toString).isEmpty)
          return "No valid Data as semi join input"
    }

      semiJoinDataAsTheyAre = vv(semi_join_dataset.toString)
      semi_join_metaDag = Some(semiJoinDataAsTheyAre.metaDag)
    }

    val select = dataAsTheyAre.add_select_statement(semi_join_metaDag, semi_join_list, selected_meta._2, selected_regions._2)

    val index = counter.getAndIncrement()
    val out_p = input_dataset + "/select" + index
    vv = vv + (out_p -> select)

    out_p

  }

  /*
    def PROJECT(projected_meta : Option[List[String]] = None,
                extended_meta : Option[List[MetaExtension]] = None,
                all_but_meta : Boolean  = false ,
                projected_values : Option[List[Int]] = None,
                all_but_reg : Option[List[String]] = None,
                extended_values : Option[List[RegionFunction]] = None): IRVariable = {

*/


  def project(projected_meta: Array[String], extended_meta: String, all_but_meta: Boolean,
              projected_region: Array[String], extended_values: Any, all_but_reg:Boolean,
              input_dataset: String): String = {

    if (vv.get(input_dataset).isEmpty)
      return "No valid Data as input"

    var regions_index: (String, Option[List[Int]]) = ("", None)
    var regions_in_but: (String, Option[List[String]]) = ("", None)
    var extended_reg: (String, Option[List[RegionFunction]]) = ("", None)
    var extended_m: (String, Option[List[MetaExtension]]) = ("", None)
    val dataAsTheyAre = vv(input_dataset)

    val meta_list: Option[List[String]] = MetadataAttributesList(projected_meta)

    if (projected_region == null)
      regions_index = ("OK", None)
    else {
      if (!all_but_reg) {
        regions_index = regionsList(projected_region, dataAsTheyAre)
        if (regions_index._2.isEmpty)
          return regions_index._1
      }
      else {
        regions_in_but = regionsList_but(projected_region, dataAsTheyAre)
        if (regions_in_but._2.isEmpty)
          return regions_in_but._1
      }
    }

    if (extended_values != null) {
      val parser = new Parser(dataAsTheyAre, GMQL_server)
      extended_reg = parser.parseProjectRegion(extended_values.toString)
      if (extended_reg._2.isEmpty)
        return extended_reg._1
    }

    if(extended_meta != null) {
      val parser = new Parser(dataAsTheyAre, GMQL_server)
      extended_m = parser.parseProjectMetdata(extended_meta.toString)
      if (extended_m._2.isEmpty)
        return extended_m._1
    }

    val project = dataAsTheyAre.PROJECT(meta_list, extended_m._2, all_but_meta, regions_index._2,
      regions_in_but._2, extended_reg._2)
    val index = counter.getAndIncrement()

    val out_p = input_dataset + "/project" + index
    vv = vv + (out_p -> project)

    out_p
  }

  def extend(metadata: Array[Array[String]], input_dataset: String): String = {
    if (vv.get(input_dataset).isEmpty)
      return "No valid Data as input"

    val dataAsTheyAre = vv(input_dataset)
    var meta_list: (String, List[RegionsToMeta]) = ("", List())

    if (metadata != null) {
      meta_list = RegionsToMetaFactory(metadata, dataAsTheyAre)
      if (meta_list._2.isEmpty)
        return meta_list._1
    }

    val extend = dataAsTheyAre.EXTEND(meta_list._2)

    val index = counter.getAndIncrement()
    val out_p = input_dataset + "/extend" + index
    vv = vv + (out_p -> extend)

    out_p
  }

  def merge(group_by:Array[Array[String]], input_dataset: String): String = {
    if (vv.get(input_dataset).isEmpty)
      return "No valid Data as input"

    val dataAsTheyAre = vv(input_dataset)

    val meta_condition_list: Option[List[AttributeEvaluationStrategy]] = MetaAttributeEvaluationStrategyList(group_by)
    val merge = dataAsTheyAre.MERGE(meta_condition_list)

    val index = counter.getAndIncrement()
    val out_p = input_dataset + "/merge" + index
    vv = vv + (out_p -> merge)

    out_p
  }

  def order(meta_order: Array[Array[String]], meta_topg: Int, meta_top: Int, meta_top_perc: Int,
            region_order: Array[Array[String]], region_topg: Int, region_top: Int, reg_top_perc: Int,
            input_dataset: String): String = {

    if (vv.get(input_dataset).isEmpty)
      return "No valid Data as input"

    val dataAsTheyAre = vv(input_dataset)

    var m_top: TopParameter = NoTop()
    var r_top: TopParameter = NoTop()

    if (meta_top > 0)
      m_top = Top(meta_top)

    if (meta_topg > 0)
      m_top = TopG(meta_topg)

    if (meta_top_perc > 0)
      m_top = TopP(meta_top_perc)

    if (region_top > 0)
      r_top = Top(region_top)

    if (region_topg > 0)
      r_top = TopG(region_topg)

    if (reg_top_perc > 0)
      r_top = TopP(reg_top_perc)


    val meta_list = meta_order_list(meta_order)
    var reg_ordering: (String, Option[List[(Int, Direction)]]) = ("", None)
    if (region_order != null) {
      reg_ordering = region_order_list(region_order, dataAsTheyAre)
      if (reg_ordering._2.isEmpty)
        return reg_ordering._1
    }
    val order = dataAsTheyAre.ORDER(meta_list, "_group", m_top, reg_ordering._2, r_top)

    val index = counter.getAndIncrement()
    val out_p = input_dataset + "/order" + index
    vv = vv + (out_p -> order)

    out_p
  }

  //we use "right" and "left" as prefixes
  def union(right_dataset: String, left_dataset: String): String = {
    if (vv.get(right_dataset).isEmpty)
      return "No valid right Data as input"

    if (vv.get(left_dataset).isEmpty)
      return "No valid left Data as input"

    val leftDataAsTheyAre = vv(left_dataset)
    val rightDataAsTheyAre = vv(right_dataset)

    val union = leftDataAsTheyAre.UNION(rightDataAsTheyAre, "left", "right")

    val index = counter.getAndIncrement()
    val out_p = left_dataset + "/union" + index
    //val out_p = left_dataset+right_dataset+"/union"+index
    vv = vv + (out_p -> union)

    out_p
  }

  def difference(join_by: Array[Array[String]], left_dataset: String, right_dataset: String, is_exact: Boolean): String = {

    if (vv.get(right_dataset).isEmpty)
      return "No valid right Data as input"

    if (vv.get(left_dataset).isEmpty)
      return "No valid left Data as input"

    val leftDataAsTheyAre = vv(left_dataset)
    val rightDataAsTheyAre = vv(right_dataset)

    val meta_condition_list: Option[MetaJoinCondition] = MetaJoinConditionList(join_by)

    val difference = leftDataAsTheyAre.DIFFERENCE(meta_condition_list, rightDataAsTheyAre, is_exact)

    val index = counter.getAndIncrement()
    //val out_p = left_dataset+right_dataset+"/difference"+index
    val out_p = left_dataset + "/difference" + index
    vv = vv + (out_p -> difference)

    out_p
  }

  /* COVER, FLAT, SUMMIT, HISTOGRAM */

  def flat(min: Any, max: Any, groupBy: Array[Array[String]], aggregates: Array[Array[String]], input_dataset: String): String = {
    val (error, flat) = doVariant(CoverFlag.FLAT, min, max, groupBy, aggregates, input_dataset)
    if (flat == null)
      return error

    val index = counter.getAndIncrement()
    val out_p = input_dataset + "/flat" + index
    vv = vv + (out_p -> flat)

    out_p
  }

  def histogram(min: Any, max: Any, groupBy: Array[Array[String]], aggregates: Array[Array[String]], input_dataset: String): String = {
    val (error, histogram) = doVariant(CoverFlag.FLAT, min, max, groupBy, aggregates, input_dataset)
    if (histogram == null)
      return error

    val index = counter.getAndIncrement()
    val out_p = input_dataset + "/histogram" + index
    vv = vv + (out_p -> histogram)

    out_p
  }

  def summit(min: Any, max: Any, groupBy: Array[Array[String]], aggregates: Array[Array[String]], input_dataset: String): String = {
    val (error, summit) = doVariant(CoverFlag.SUMMIT, min, max, groupBy, aggregates, input_dataset)
    if (summit == null)
      return error


    val index = counter.getAndIncrement()
    val out_p = input_dataset + "/summit" + index
    vv = vv + (out_p -> summit)

    out_p
  }

  def cover(min: Any, max: Any, groupBy: Array[Array[String]], aggregates: Array[Array[String]], input_dataset: String): String = {
    val (error, cover) = doVariant(CoverFlag.COVER, min, max, groupBy, aggregates, input_dataset)
    if (cover == null)
      return error

    val index = counter.getAndIncrement()
    val out_p = input_dataset + "/cover" + index
    vv = vv + (out_p -> cover)

    out_p
  }

  def doVariant(flag: CoverFlag.CoverFlag, min: Any, max: Any, groupBy: Array[Array[String]],
                aggregates: Array[Array[String]], input_dataset: String): (String, IRVariable) = {
    if (vv.get(input_dataset).isEmpty)
      return ("No valid dataset as input", null)

    val dataAsTheyAre = vv(input_dataset)
    var aggr_list: (String, List[RegionsToRegion]) = ("", List())
    var paramMin = get_param(min)
    if (paramMin._2 == null)
      return (paramMin._1, null)

    var paramMax = get_param(max)
    if (paramMax._2 == null)
      return (paramMax._1, null)

    if (aggregates != null) {
     // println("not null aggregates")
      aggr_list = RegionToRegionAggregates(aggregates, dataAsTheyAre)
      if (aggr_list._2.isEmpty)
        return (aggr_list._1, null)
    }
    //else
     // println("null aggregates")

    val groupList: Option[List[AttributeEvaluationStrategy]] = MetaAttributeEvaluationStrategyList(groupBy)

    val variant = dataAsTheyAre.COVER(flag, paramMin._2, paramMax._2, aggr_list._2, groupList)

    ("OK", variant)
  }


  // we do not add left, right and count name: we set to None
  def map(condition: Array[Array[String]], aggregates: Array[Array[String]], right_dataset: String, left_dataset: String): String = {
    if (vv.get(right_dataset).isEmpty)
      return "No valid right Data as input"

    if (vv.get(left_dataset).isEmpty)
      return "No valid left Data as input"

    val leftDataAsTheyAre = vv(left_dataset)
    val rightDataAsTheyAre = vv(right_dataset)

    var aggr_list: (String, List[RegionsToRegion]) = ("", List())

    if (aggregates != null) {
      aggr_list = RegionToRegionAggregates(aggregates, leftDataAsTheyAre)
      if (aggr_list._2.isEmpty)
        return aggr_list._1
    }

    val condition_list: Option[MetaJoinCondition] = MetaJoinConditionList(condition)

    val map = leftDataAsTheyAre.MAP(condition_list, aggr_list._2, rightDataAsTheyAre, None, None, None)

    val index = counter.getAndIncrement()
    val out_p = left_dataset + right_dataset + "/map" + index
    vv = vv + (out_p -> map)

    out_p
  }


  // we do not add ref and exp name: we set to None
  def join(region_join: Array[Array[String]], meta_join: Array[Array[String]], output: String, right_dataset: String, left_dataset: String): String = {
    if (vv.get(right_dataset).isEmpty)
      return "No valid right Data as input"

    if (vv.get(left_dataset).isEmpty)
      return "No valid left Data as input"

    val leftDataAsTheyAre = vv(left_dataset)
    val rightDataAsTheyAre = vv(right_dataset)

    val meta_join_list: Option[MetaJoinCondition] = MetaJoinConditionList(meta_join)
    val region_join_list: List[JoinQuadruple] = RegionQuadrupleList(region_join)

    val reg_out = regionBuild(output)

    val join = leftDataAsTheyAre.JOIN(meta_join_list, region_join_list, reg_out, rightDataAsTheyAre, None, None)

    val index = counter.getAndIncrement()
    // val out_p = left_dataset+right_dataset+"/join"+index
    val out_p = left_dataset + "/join" + index
    vv = vv + (out_p -> join)

    out_p
  }


  /*UTILS FUNCTION*/

  def get_param(param: Any): (String, CoverParam) = {
    var covParam: CoverParam = null
    var parser_res: (String, CoverParam)=null
    param match {
      case param: Int => covParam = new N {
          override val n: Int = param
        }
      case param: String => {
        val parser = new Parser()
        parser_res = parser.parseCoverParam(param)
        if (parser_res._2 == null)
          return ("No valid min or max input", parser_res._2)

        covParam = parser_res._2
      }
    }

    ("OK", covParam)
  }

  def regionBuild(output: String): RegionBuilder = {

    output match {
      case "LEFT" => RegionBuilder.LEFT
      case "RIGHT" => RegionBuilder.RIGHT
      case "CONTIG" => RegionBuilder.CONTIG
      case "INTERSECTION" => RegionBuilder.INTERSECTION
    }

  }


  def RegionsToMetaFactory(aggregates: Any, data: IRVariable): (String, List[RegionsToMeta]) = {
    var region_meta_list: List[RegionsToMeta] = List()
    var temp_list = new ListBuffer[RegionsToMeta]()

    aggregates match {
      case aggregates: Array[Array[String]] => {

        for (elem <- aggregates) {
          if (elem(1).equalsIgnoreCase("COUNT"))
            temp_list += DefaultRegionsToMetaFactory.get(elem(1), Some(elem(0)))
          else {
            val field = data.get_field_by_name(elem(2))
            if (field.isEmpty) {
              val error = "No value " + elem(2) + " from this schema"
              return (error, region_meta_list) //empty list
            }
            temp_list += DefaultRegionsToMetaFactory.get(elem(1), field.get, Some(elem(0)))
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
            temp_list += DefaultRegionsToRegionFactory.get(elem(1), Some(elem(0)))
          else {
            val field = data.get_field_by_name(elem(2))
            if (field.isEmpty) {
              val error = "No value " + elem(2) + " from this schema"
              return (error, aggr_list) //empty list
            }
            temp_list += DefaultRegionsToRegionFactory.get(elem(1), field.get, Some(elem(0)))
          }
        }
        aggr_list = temp_list.toList
      }
    }
    ("OK", aggr_list) // not empty list
  }


  def MetadataAttributesList(group_by: Array[String]): Option[List[String]] = {
    var group_list: Option[List[String]] = None
    val temp_list = new ListBuffer[String]()

    if(group_by==null) {
      println("null")
      return group_list
    }
    println("not null")
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
      //println("null")
      return join_by_list
    }
   // println("not null")

    for (elem <- join_by) {
      val attribute = elem(0)
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
      //println("null")
      return join_by_list
    }
    //println("not null")

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


  def RegionQuadrupleList(quad_join_list: Any): List[JoinQuadruple] = {

    var quad_list: List[JoinQuadruple] = null
    val temp_list = new ListBuffer[JoinQuadruple]()

    if (quad_join_list == null)
      return quad_list


    quad_join_list match {
      case quad_matrix: Array[Array[String]] => {
        for (elem <- quad_matrix) {
          temp_list += JoinQuadruple(first = atomic_cond(elem(0), elem(1)),
            atomic_cond(elem(2), elem(3)), atomic_cond(elem(4), elem(5)),
            atomic_cond(elem(6), elem(7)))
        }
        quad_list = temp_list.toList
      }
    }

    quad_list
  }


  def atomic_cond(cond: String, value: String): Option[AtomicCondition] = {
    cond match {
      case "DGE" => Some(DistGreater(value.toInt))
      case "DLE" => Some(DistLess(value.toInt))
      case "UP" => Some(Upstream())
      case "DOWN" => Some(DownStream())
      case "MD" => Some(MinDistance(value.toInt))
      case "NA" => None
    }
  }

  def main(args: Array[String]): Unit =
  {
    initGMQL("TAB", false)
    var r = readDataset("/Users/simone/Downloads/DATA_SET_VAR_GTF","CUSTOMPARSER",true,null)
    val t = take(r,3)
  }

}
