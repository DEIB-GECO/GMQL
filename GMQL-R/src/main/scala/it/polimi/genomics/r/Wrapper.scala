package it.polimi.genomics.r

import java.io.FileNotFoundException
import java.util.concurrent.atomic.AtomicLong

import it.polimi.genomics.GMQLServer.{DefaultRegionsToMetaFactory, DefaultRegionsToRegionFactory, GmqlServer}
import it.polimi.genomics.core.DataStructures.CoverParameters._
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor.GMQL_DATASET
import it.polimi.genomics.core.DataStructures.CoverParameters.CoverFlag.CoverFlag
import it.polimi.genomics.core.DataStructures.GroupMDParameters.Direction.Direction
import it.polimi.genomics.core.DataStructures.GroupMDParameters._
import it.polimi.genomics.core.DataStructures.JoinParametersRD._
import it.polimi.genomics.core.DataStructures.JoinParametersRD.RegionBuilder.RegionBuilder
import it.polimi.genomics.core.DataStructures.{IRVariable, MetaOperator}
import it.polimi.genomics.core.DataStructures.MetaJoinCondition._
import it.polimi.genomics.core.DataStructures.MetadataCondition.MetadataCondition
import it.polimi.genomics.core.DataStructures.RegionAggregate.{RegionsToMeta, RegionsToRegion}
import it.polimi.genomics.core.DataStructures.RegionCondition.RegionCondition
import it.polimi.genomics.core.{GMQLSchemaFormat, GRecordKey, GValue}
import it.polimi.genomics.core.GMQLSchemaFormat.Value
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import it.polimi.genomics.spark.implementation.loaders._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import scala.collection.mutable.ListBuffer

/**
  * Created by simone on 21/03/17.
  */

object Wrapper {
  var GMQL_server: GmqlServer = _

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

  var mem_meta: Array[Array[String]] = null
  var mem_regions: Array[Array[String]] = null
  var mem_schema: Array[Array[String]] = null

  var vv: Map[String, IRVariable] = Map[String, IRVariable]()

  def initGMQL(output_format: String): Unit = {
    val spark_conf = new SparkConf().setMaster("local[*]").
      setAppName("GMQL-R").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.executor.memory", "6g")
      .set("spark.driver.memory", "2g")
    val spark_context = new SparkContext(spark_conf)

    val out_format = outputFormat(output_format)

    val executor = new GMQLSparkExecutor(sc = spark_context, outputFormat = out_format)
    GMQL_server = new GmqlServer(executor)

    if (GMQL_server == null) {
      println("GMQL Server is down")
      return
    }

    println("GMQL Server is up")
  }

  def outputFormat(format: String): GMQLSchemaFormat.Value = {
    format match {
      case "TAB" => GMQLSchemaFormat.TAB
      case "GTF" => GMQLSchemaFormat.GTF
      case "VCF" => GMQLSchemaFormat.VCF
      case "COLLECT" => GMQLSchemaFormat.COLLECT
    }
  }

  def readDataset(data_input_path: String, parser_name: String, local: Boolean): String = {
    var parser: BedParser = null
    var out_p = ""
    if (local) {
      val data_path = data_input_path + "/files"

      parser_name match {
        case "BedParser" => parser = BedParser
        case "ANNParser" => parser = ANNParser
        case "BroadProjParser" => parser = BroadProjParser
        case "BasicParser" => parser = BasicParser
        case "NarrowPeakParser" => parser = NarrowPeakParser
        case "RnaSeqParser" => parser = RnaSeqParser
        case "CustomParser" => {
          parser = new CustomParser()
          try {
            parser.asInstanceOf[CustomParser].setSchema(data_path)
          }
          catch {
            case fe: FileNotFoundException => return fe.getMessage
          }
        }
        case _ => return "No parser defined"
      }

      val dataAsTheyAre = GMQL_server.READ(data_path).USING(parser)
      val index = counter.getAndIncrement()
      out_p = "dataset" + index
      vv = vv + (out_p -> dataAsTheyAre)

      out_p
    }
    else {
      out_p
    }
  }

  def materialize(data_to_materialize: String, data_output_path: String): String = {
    if (vv.get(data_to_materialize).isEmpty)
      return "No valid Data to materialize"

    val materialize = vv(data_to_materialize)
    GMQL_server setOutputPath data_output_path MATERIALIZE materialize
    materialize_count.getAndIncrement()

    "OK"
  }

  def execute(): String = {
    if (materialize_count.get() <= 0)
      "You must materialize before"
    else {
      GMQL_server.run()
      materialize_count.set(0)
      "OK"
    }

  }

  def take(data_to_take: String, how_many: Int): String = {
    if (vv.get(data_to_take).isEmpty)
      return "No valid Data to materialize"

    var output: Any = null
    val taken = vv(data_to_take)

    if (how_many == 0)
      output = GMQL_server.setOutputPath("").COLLECT(taken)
    else
      output = GMQL_server.setOutputPath("").TAKE(taken, how_many)

    mem_regions = output.asInstanceOf[GMQL_DATASET]._1.
      map(x => Array[String](x._1._1.toString, x._1._2, x._1._3.toString, x._1._4.toString, x._1._5.toString) ++ x._2.
        map(s => s.toString))

    for(reg <- mem_regions)
      {
        print(reg.mkString(" "))
        println()
      }

    mem_meta = output.asInstanceOf[GMQL_DATASET]._2.
      map(x => Array[String](x._1.toString, x._2._1, x._2._2))

    for(reg <- mem_meta)
    {
      print(reg.mkString(" "))
      println()
    }

    mem_schema = output.asInstanceOf[GMQL_DATASET]._3.
      map(x => Array[String](x._1,x._2.toString)).toArray

    for(reg <- mem_schema)
    {
      print(reg.mkString(" "))
      println()
    }

    "OK"
  }

  def get_reg(): Array[Array[String]] = {
    if(mem_regions==null)
      return Array(Array("NO regions in memory"))
    mem_regions
  }

  def get_meta(): Array[Array[String]] = {
    if(mem_meta==null)
      return Array(Array("NO metadata in memory"))
    mem_meta
  }

  def get_schema(): Array[Array[String]] = {
    if(mem_schema==null)
      return Array(Array("NO schema in memory"))
    mem_schema
  }


  /*GMQL OPERATION*/

  //TODO no check on regions
  def select(predicate: Any, region_predicate: Any, semi_join: Any, semi_join_dataset: Any, input_dataset: String): String = {
    if (vv.get(input_dataset).isEmpty)
      return "No valid Data as input"

    val dataAsTheyAre = vv(input_dataset)

    var semiJoinDataAsTheyAre: IRVariable = null
    var semi_join_metaDag: Option[MetaOperator] = None
    var metadata: (String, Option[MetadataCondition]) = ("", None)
    var regions: (String, Option[RegionCondition]) = ("", None)
    val parser = new Parser()

    if (predicate != null) {
      metadata = parser.parseSelectMetadata(predicate.toString)
      if (metadata._2.isEmpty)
        return metadata._1
    }

    if (region_predicate != null) {
      regions = parser.parseSelectRegions(region_predicate.toString)
      if (regions._2.isEmpty)
        return regions._1
    }

    val semi_join_list = MetaJoinConditionList(semi_join)
    if (semi_join_list == null)
      return "No valid condition in semi join"

    if (semi_join_list.isDefined) {
      if (vv.get(semi_join_dataset.toString).isEmpty)
        return "No valid Data as semi join input"

      semiJoinDataAsTheyAre = vv(semi_join_dataset.toString)
      semi_join_metaDag = Some(semiJoinDataAsTheyAre.metaDag)
    }

    val select = dataAsTheyAre.add_select_statement(semi_join_metaDag, semi_join_list, metadata._2, regions._2)

    val index = counter.getAndIncrement()
    val out_p = input_dataset + "/select" + index
    vv = vv + (out_p -> select)

    out_p

  }

  /* PROJECT(projected_meta : Option[List[String]] = None, extended_meta : Option[MetaAggregateStruct] = None,
              projected_values : Option[List[Int]] = None,
              extended_values : Option[List[RegionFunction]] = None): */

  //TODO miss parsing on region and metdata update
  def project(projected_meta: Any, projected_region: Any, input_dataset: String): String = {
    if (vv.get(input_dataset).isEmpty)
      return "No valid Data as input"

    val dataAsTheyAre = vv(input_dataset)

    val meta_list: Option[List[String]] = MetadataAttributesList(projected_meta)
    val (error, regions_index_list) = regionsList(projected_region, dataAsTheyAre)
    if (regions_index_list.isEmpty)
      return error


    val project = dataAsTheyAre.PROJECT(meta_list, None, regions_index_list, None)
    val index = counter.getAndIncrement()

    val out_p = input_dataset + "/project" + index
    vv = vv + (out_p -> project)

    out_p
  }

  def extend(metadata: Any, input_dataset: String): String = {
    if (vv.get(input_dataset).isEmpty)
      return "No valid Data as input"

    val dataAsTheyAre = vv(input_dataset)

    val (error, meta_list) = RegionsToMetaFactory(metadata, dataAsTheyAre)
    if (meta_list.isEmpty)
      return error

    val extend = dataAsTheyAre.EXTEND(meta_list)

    val index = counter.getAndIncrement()
    val out_p = input_dataset + "/extend" + index
    vv = vv + (out_p -> extend)

    out_p
  }

  def merge(group_by: Any, input_dataset: String): String = {
    if (vv.get(input_dataset).isEmpty)
      return "No valid dataset as input"

    val dataAsTheyAre = vv(input_dataset)

    val group_list: Option[List[String]] = MetadataAttributesList(group_by)
    val merge = dataAsTheyAre.MERGE(group_list)

    val index = counter.getAndIncrement()
    val out_p = input_dataset + "/merge" + index
    vv = vv + (out_p -> merge)

    out_p
  }

  def order(meta_order: Any, meta_topg: Int, meta_top: Int,
            region_order: Any, region_topg: Int, region_top: Int, input_dataset: String): String = {
    if (vv.get(input_dataset).isEmpty)
      return "No valid dataset as input"

    val dataAsTheyAre = vv(input_dataset)

    var m_top: TopParameter = NoTop()
    var r_top: TopParameter = NoTop()

    if (meta_top > 0)
      m_top = Top(meta_top)

    if (meta_topg > 0)
      m_top = TopG(meta_topg)

    if (region_top > 0)
      r_top = Top(region_top)

    if (region_topg > 0)
      r_top = TopG(region_topg)


    val meta_list = meta_order_list(meta_order)
    val (error, region_list) = region_order_list(region_order, dataAsTheyAre)
    if (region_list.isEmpty)
      return error

    val order = dataAsTheyAre.ORDER(meta_list, "_group", m_top, region_list, r_top)

    val index = counter.getAndIncrement()
    val out_p = input_dataset + "/order" + index
    vv = vv + (out_p -> order)

    out_p
  }

  //we use "right" and "left" as prefixes
  def union(right_dataset: String, left_dataset: String): String = {
    if (vv.get(right_dataset).isEmpty)
      return "No valid right dataset as input"

    if (vv.get(left_dataset).isEmpty)
      return "No valid left dataset as input"

    val leftDataAsTheyAre = vv(left_dataset)
    val rightDataAsTheyAre = vv(right_dataset)


    val union = leftDataAsTheyAre.UNION(rightDataAsTheyAre, "left", "right")

    val index = counter.getAndIncrement()
    val out_p = left_dataset + "/union" + index
    //val out_p = left_dataset+right_dataset+"/union"+index
    vv = vv + (out_p -> union)

    out_p
  }

  def difference(join_by: Any, left_dataset: String, right_dataset: String): String = {
    if (vv.get(right_dataset).isEmpty)
      return "No valid right dataset as input"

    if (vv.get(left_dataset).isEmpty)
      return "No valid left dataset as input"

    val leftDataAsTheyAre = vv(left_dataset)
    val rightDataAsTheyAre = vv(right_dataset)

    val join_by_list: Option[MetaJoinCondition] = MetaJoinConditionList(join_by)

    val difference = leftDataAsTheyAre.DIFFERENCE(join_by_list, rightDataAsTheyAre)

    val index = counter.getAndIncrement()
    //val out_p = left_dataset+right_dataset+"/difference"+index
    val out_p = left_dataset + "/difference" + index
    vv = vv + (out_p -> difference)

    out_p
  }

  /* COVER, FLAT, SUMMIT, HISTOGRAM */

  def flat(min: Int, max: Int, groupBy: Any, aggregates: Any, input_dataset: String): String = {
    val (error, flat) = doVariant(CoverFlag.FLAT, min, max, groupBy, aggregates, input_dataset)
    if (flat == null)
      return error

    val index = counter.getAndIncrement()
    val out_p = input_dataset + "/flat" + index
    vv = vv + (out_p -> flat)

    out_p
  }

  def histogram(min: Int, max: Int, groupBy: Any, aggregates: Any, input_dataset: String): String = {
    val (error, histogram) = doVariant(CoverFlag.FLAT, min, max, groupBy, aggregates, input_dataset)
    if (histogram == null)
      return error

    val index = counter.getAndIncrement()
    val out_p = input_dataset + "/histogram" + index
    vv = vv + (out_p -> histogram)

    out_p
  }

  def summit(min: Int, max: Int, groupBy: Any, aggregates: Any, input_dataset: String): String = {
    val (error, summit) = doVariant(CoverFlag.SUMMIT, min, max, groupBy, aggregates, input_dataset)
    if (summit == null)
      return error


    val index = counter.getAndIncrement()
    val out_p = input_dataset + "/summit" + index
    vv = vv + (out_p -> summit)

    out_p
  }

  def cover(min: Int, max: Int, groupBy: Any, aggregates: Any, input_dataset: String): String = {
    val (error, cover) = doVariant(CoverFlag.COVER, min, max, groupBy, aggregates, input_dataset)
    if (cover == null)
      return error

    val index = counter.getAndIncrement()
    val out_p = input_dataset + "/cover" + index
    vv = vv + (out_p -> cover)

    out_p
  }

  def doVariant(flag: CoverFlag.CoverFlag, min: Int, max: Int, groupBy: Any,
                aggregates: Any, input_dataset: String): (String, IRVariable) = {
    if (vv.get(input_dataset).isEmpty)
      return ("No valid dataset as input", null)

    val dataAsTheyAre = vv(input_dataset)

    var paramMin: CoverParam = null
    var paramMax: CoverParam = null

    var aggrlist: List[RegionsToRegion] = null

    min match {
      case 0 => paramMin = new ANY {}
      case -1 => paramMin = new ALL {}
      case x if x > 0 => paramMin = new N {
        override val n: Int = min
      }
    }

    max match {
      case 0 => paramMax = new ANY {}
      case -1 => paramMax = new ALL {}
      case x if x > 0 => paramMax = new N {
        override val n: Int = max
      }
    }

    val (error, aggr_list) = RegionToRegionAggregates(aggregates, dataAsTheyAre)
    if (aggr_list.isEmpty)
      return (error, null)

    val groupList: Option[List[String]] = MetadataAttributesList(groupBy)

    val variant = dataAsTheyAre.COVER(flag, paramMin, paramMax, aggr_list, groupList)

    ("OK", variant)
  }


  // we do not add left, right and count name: we set to None
  def map(condition: Any, aggregates: Any, right_dataset: String, left_dataset: String): String = {
    if (vv.get(right_dataset).isEmpty)
      return "No valid right dataset as input"

    if (vv.get(left_dataset).isEmpty)
      return "No valid left dataset as input"

    val leftDataAsTheyAre = vv(left_dataset)
    val rightDataAsTheyAre = vv(right_dataset)

    val (error, aggr_list) = RegionToRegionAggregates(aggregates, leftDataAsTheyAre)
    if (aggr_list == null)
      return error

    val condition_list: Option[MetaJoinCondition] = MetaJoinConditionList(condition)

    val map = leftDataAsTheyAre.MAP(condition_list, aggr_list, rightDataAsTheyAre, None, None, None)

    val index = counter.getAndIncrement()
    val out_p = left_dataset + right_dataset + "/map" + index
    vv = vv + (out_p -> map)

    out_p
  }


  // we do not add ref and exp name: we set to None
  def join(region_join: Any, meta_join: Any, output: String, right_dataset: String, left_dataset: String): String = {
    if (vv.get(right_dataset).isEmpty)
      return "No valid right dataset as input"

    if (vv.get(left_dataset).isEmpty)
      return "No valid left dataset as input"

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

    if (aggregates == null) {
      return ("No", region_meta_list)
    }

    val temp_list = new ListBuffer[RegionsToMeta]()

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

    if (aggregates == null) {
      return ("No", aggr_list)
    }
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


  def MetadataAttributesList(group_by: Any): Option[List[String]] = {
    var group_list: Option[List[String]] = None
    val temp_list = new ListBuffer[String]()

    if (group_by == null)
      return group_list

    group_by match {
      case group_by: String => {
        val temp: Array[String] = Array(group_by)
        group_list = Some(temp.toList)
      }
      case group_by: Array[String] => {
        for (elem <- group_by)
          temp_list += elem

        if (temp_list.nonEmpty)
          group_list = Some(temp_list.toList)
      }
    }
    group_list
  }


  def MetaJoinConditionList(join_by: Any): Option[MetaJoinCondition] = {
    var join_by_list: Option[MetaJoinCondition] = None
    val joinList = new ListBuffer[AttributeEvaluationStrategy]()

    if (join_by == null) {
      return join_by_list
    }

    join_by match {
      case join_by: Array[Array[String]] => {
        for (elem <- join_by) {
          val attribute = elem(0)
          attribute match {
            case "DEF" => joinList += Default(elem(1))
            case "FULL" => joinList += FullName(elem(1))
            case "EXACT" => joinList += Exact(elem(1))
            case _ => return null
          }
        }
        if (joinList.nonEmpty)
          join_by_list = Some(MetaJoinCondition(joinList.toList))
      }
    }
    join_by_list
  }


  def regionsList(projected_by: Any, data: IRVariable): (String, Option[List[Int]]) = {
    var projectedList: Option[List[Int]] = None
    val temp_list = new ListBuffer[Int]()

    if (projected_by == null)
      return ("No", projectedList)

    projected_by match {
      case projected_by: String =>
        projected_by match {
          case "" => projectedList = None //println("groupBy is single string but empty")}
          case _ => {
            val field = data.get_field_by_name(projected_by)
            projectedList = Some(field.toList)
            // println(groupBy)
            // println("groupBy is single string")
          }
        }
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

    if (order_matrix == null)
      return ("No", order_list)

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

    return quad_list
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

  def main(args: Array[String]): Unit = {

    initGMQL("TAB")
    val r = readDataset("/Users/simone/Downloads/DATA_SET_VAR_GTF", "CustomParser", true)
    take(r,10)
  }

}
