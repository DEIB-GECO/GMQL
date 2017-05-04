package it.polimi.genomics.r

import java.util.concurrent.atomic.AtomicLong

import it.polimi.genomics.GMQLServer.{DefaultRegionsToMetaFactory, DefaultRegionsToRegionFactory, GmqlServer}
import it.polimi.genomics.core.DataStructures
import it.polimi.genomics.core.DataStructures.CoverParameters.{ALL, ANY, CoverFlag, CoverParam, N}
import it.polimi.genomics.core.DataStructures.IRVariable
import it.polimi.genomics.core.DataStructures.RegionAggregate.{RegionsToMeta, RegionsToRegion}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import it.polimi.genomics.spark.implementation.loaders.CustomParser
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import scala.collection.mutable.ListBuffer

/**
  * Created by simone on 21/03/17.
  */

object Wrapper
{
  var GMQLServer: GmqlServer = _

  //thread safe counter for unique string pointer to dataset
  //'cause we could have two pointer at the same dataset and we
  //can not distinguish them

  //example:
  // R shell
  //r = readDataset("/Users/simone/Downloads/job_filename_guest_new14_20170316_162715_DATA_SET_VAR")
  //r1 = readDataset("/Users/simone/Downloads/job_filename_guest_new14_20170316_162715_DATA_SET_VAR")
  //are mapped in vv with the same key
  var counter: AtomicLong = new AtomicLong(0)
  var materializeCount: AtomicLong = new AtomicLong(0)

  var vv: Map[String,IRVariable] = Map[String,IRVariable]()

  def startGMQL(): Unit =
  {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("GMQL-R")
    val sparkContext = new SparkContext(sparkConf)
    val executor = new GMQLSparkExecutor(sc = sparkContext)
    GMQLServer = new GmqlServer(executor)
    if(GMQLServer==null)
    {
      println("GMQLServer is null")
      return
    }
    println("GMQL server is up")
  }

  def readDataset(data_input_path: Array[String]): String =
  {
    val parser = new CustomParser()
    val dataPath  = data_input_path(0)+ "/files"
    parser.setSchema(dataPath)

    val dataAsTheyAre = GMQLServer.READ(dataPath).USING(parser)

    val index = counter.getAndIncrement()
    val out_p = "dataset"+index
    vv = vv + (out_p -> dataAsTheyAre)

    out_p
  }

  def materialize(data_to_materialize: Array[String], data_output_path: Array[String]): Unit =
  {
    val materialize = vv(data_to_materialize(0))
    GMQLServer setOutputPath data_output_path(0) MATERIALIZE materialize
    materializeCount.getAndIncrement()
  }

  def execute(): String =
  {
    if(materializeCount.get() <=0) {
      return "you must materialize before"
    }

    else{
      GMQLServer.run()
      materializeCount.set(0)
      return "OK"
    }

  }


  /*GMQL OPERATION*/

  def select(predicate:String,region:String,semijoin:Any, input_dataset: String): String =
  {
    val dataAsTheyAre = vv(input_dataset)

    val t = new PM()
    val (msg,meta_con) = t.parsaMetadati(predicate)
    if (msg!="OK") {
      return msg
    }

    val   reg_con =
    //        DataStructures.RegionCondition.OR(
    //          DataStructures.RegionCondition.Predicate(3, DataStructures.RegionCondition.REG_OP.GT, 30),
    //DataStructures.RegionCondition.Predicate(0, DataStructures.RegionCondition.REG_OP.EQ, "400")
      DataStructures.RegionCondition.Predicate(2, DataStructures.RegionCondition.REG_OP.GT, DataStructures.RegionCondition.MetaAccessor("cell"))
    //        )

    val select = dataAsTheyAre.SELECT(meta_con,reg_con)
    val out_p = input_dataset+"/select"
    vv = vv + (out_p -> select)
    out_p

  }

  def project(input_dataset: String): String =
  {
    //TODO
    val dataAsTheyAre = vv(input_dataset)

    val project = dataAsTheyAre.PROJECT()
    val index = counter.getAndIncrement()

    val out_p = input_dataset + "/project"+ index
    vv = vv + (out_p -> project)

    out_p
  }

  def extend(metadata:Array[Array[String]], input_dataset: String): String =
  {
    val dataAsTheyAre = vv(input_dataset)

    val (error, metaList) = RegionToMetaAggregates(metadata, dataAsTheyAre)
    if (metaList == null)
      return error

    val extend = dataAsTheyAre.EXTEND(metaList)
    val index = counter.getAndIncrement()

    val out_p = input_dataset + "/extend"+ index
    vv = vv + (out_p -> extend)

    out_p
  }

  def group(groupBy:Any,metaAggregates:List[Array[String]],regionGroup:Any,
            regionAggregates:Any, input_dataset: Array[String]): String = {
    //TODO
    /*  def GROUP(meta_keys : Option[MetaGroupByCondition] = None,
    meta_aggregates : Option[List[RegionsToMeta]] = None,
    meta_group_name : String = "_group",
    region_keys : Option[List[GroupingParameter]],
    region_aggregates : Option[List[RegionsToRegion]])
    */

    val dataAsTheyAre = vv(input_dataset(0))
    //val groupList: Option[List[String]] = AttributesList(groupBy)

    //val metaAggrList = RegionToMetaAggregates(metaAggregates,dataAsTheyAre)
    //val regionAggrList = RegionToRegionAggregates(metaAggregates,dataAsTheyAre)

    val group = dataAsTheyAre.GROUP(None, None, "_group", None, None)

    val out_p = input_dataset + "/group"
    vv = vv + (out_p -> group)

    out_p
  }

  /*GMQL MERGE*/
  def merge(groupBy:Any, input_dataset: Array[String]): String =
  {
    val dataAsTheyAre = vv(input_dataset(0))
    val groupList: Option[List[String]] = AttributesList(groupBy)
    val merge = dataAsTheyAre.MERGE(groupList)

    val index = counter.getAndIncrement()
    val out_p = input_dataset+"/merge"+index
    vv = vv + (out_p -> merge)

    out_p
  }

  def order(input_dataset: String): Unit =
  {
    //val dataAsTheyAre = vv(input_dataset)
    //TODO
  }

  /*GMQL UNION*/
  def union(right_dataset: Array[String], left_dataset: Array[String]): String =
  {
    val leftDataAsTheyAre = vv(left_dataset(0))
    val rightDataAsTheyAre = vv(right_dataset(0))

    //prefix both right and left leave default value
    val union = leftDataAsTheyAre.UNION(rightDataAsTheyAre)

    val out_p = left_dataset(0)+right_dataset(0)+"/union"
    vv = vv + (out_p -> union)

    out_p
  }

  //TODO: metajoinCondition?
  def difference(joinBy:Any,left_dataset: String,right_dataset: String): String =
  {
    val leftDataAsTheyAre = vv(left_dataset)
    val rightDataAsTheyAre = vv(right_dataset)

    //val joinByList: Option[List[String]] = AttributesList(joinBy)

    val difference = leftDataAsTheyAre.DIFFERENCE(None,rightDataAsTheyAre)

    val out_p = left_dataset+right_dataset+"/difference"
    vv = vv + (out_p -> difference)

    out_p
  }

  /* GMQL COVER, FLAT, SUMMIT, HISTOGRAM */

  def flat(min:Array[Int] , max:Array[Int], groupBy:Any,aggregates:Array[Array[String]],
           input_dataset: Array[String]): String =
  {
    val (error,flat) = doVariant(CoverFlag.FLAT,min,max,groupBy,aggregates,input_dataset)
    if(flat==null)
      return error

    val out_p = input_dataset+"/flat"
    vv = vv + (out_p -> flat)

    out_p
  }

  def histogram(min:Array[Int] , max:Array[Int], groupBy:Any,aggregates:Array[Array[String]],
                input_dataset: Array[String]): String =
  {
    val (error,histogram) = doVariant(CoverFlag.FLAT,min,max,groupBy,aggregates,input_dataset)
    if(histogram==null)
      return error

    val out_p = input_dataset+"/histogram"
    vv = vv + (out_p -> histogram)

    out_p
  }

  def summit(min:Array[Int] , max:Array[Int], groupBy:Any,aggregates:Array[Array[String]],
             input_dataset: Array[String]): String =
  {
    val (error,summit) = doVariant(CoverFlag.SUMMIT,min,max,groupBy,aggregates,input_dataset)
    if(summit==null)
      return error

    val out_p = input_dataset+"/summit"
    vv = vv + (out_p -> summit)

    out_p
  }

  def cover(min:Array[Int] , max:Array[Int], groupBy:Any,aggregates:Any,
            input_dataset: Array[String]): String =
  {
    val (error,cover) = doVariant(CoverFlag.COVER,min,max,groupBy,aggregates,input_dataset)
    if(cover==null)
      return error

    val out_p = input_dataset+"/cover"
    vv = vv + (out_p -> cover)

    out_p
  }

  def doVariant(flag:CoverFlag.CoverFlag, min:Array[Int] , max:Array[Int], groupBy:Any,
                      aggregates:Any, input_dataset: Array[String]): (String,IRVariable) =
  {
    val dataAsTheyAre = vv(input_dataset(0))
    var paramMin:CoverParam = null
    var paramMax:CoverParam = null
    var aggrlist: List[RegionsToRegion] = null

    min(0) match {
      case 0 => paramMin = ANY()
      case -1 => paramMin = ALL()
      case x if x > 0 => paramMin = N(min(0))
    }

    max(0) match {
      case 0 => paramMax = ANY()
      case -1 => paramMax = ALL()
      case x if x > 0 => paramMax = N(max(0))
    }

    if(aggregates == null) {
      aggrlist = List()
    }
    else {
      aggregates match {
        case aggregates: Array[Array[String]] =>
          val (error, aggrlist) = RegionToRegionAggregates(aggregates, dataAsTheyAre)
          if (aggrlist == null)
            return (error,null);
      }
    }

    val groupList: Option[List[String]] = AttributesList(groupBy)

    val variant = dataAsTheyAre.COVER(flag, paramMin, paramMax, aggrlist, groupList)

    ("OK",variant)
  }


  def map(aggregates:Array[Array[String]],right_dataset: Array[String],exp_name: Array[String],
          left_dataset: Array[String], ref_name: Array[String],count_name: Array[String]): String =
  {
    //TODO

    val leftDataAsTheyAre = vv(left_dataset(0))
    val rightDataAsTheyAre = vv(right_dataset(0))

    val (error, aggrlist) = RegionToRegionAggregates(aggregates,leftDataAsTheyAre)
    if(aggrlist==null)
      return error

    val map = leftDataAsTheyAre.MAP(None,aggrlist,rightDataAsTheyAre,Option(ref_name(0)),Option(exp_name(0)),Option(count_name(0)))
    val out_p = left_dataset(0)+right_dataset(0)+"/map"
    vv = vv + (out_p -> map)

    out_p
  }

  def join(input_dataset: String):Unit =
  {
    //TODO

  }


/*UTILS FUNCTION*/

  def RegionToMetaAggregates(aggregates:Array[Array[String]],data:IRVariable): (String,List[RegionsToMeta]) =
  {
    var list:List[RegionsToMeta] = List()

    val aggrList =  new ListBuffer[RegionsToMeta]()

    for (elem <- aggregates) {
      if (elem(1).equalsIgnoreCase("COUNT"))
        aggrList += DefaultRegionsToMetaFactory.get(elem(1), Some(elem(0)))
      else
      {
        val field = data.get_field_by_name(elem(2))
        if(field.isEmpty) {
          val error = "The value "+elem(2)+" is missing from schema"
          return (error,null)
        }
        aggrList += DefaultRegionsToMetaFactory.get(elem(1), field.get, Some(elem(0)))
      }

      list = aggrList.toList
    }

    ("OK",list)

  }

  def RegionToRegionAggregates(aggregates:Array[Array[String]],data:IRVariable): (String, List[RegionsToRegion]) =
  {
    var list:List[RegionsToRegion] = List()

    val aggrList =  new ListBuffer[RegionsToRegion]()

    for (elem <- aggregates) {
        if (elem(1).equalsIgnoreCase("COUNT"))
          aggrList += DefaultRegionsToRegionFactory.get(elem(1), Some(elem(0)))
        else
        {
          val field = data.get_field_by_name(elem(2))
          if(field.isEmpty){
            val error = "The value "+elem(2)+" is missing from schema"
            return (error,null)
          }
          aggrList += DefaultRegionsToRegionFactory.get(elem(1), field.get, Some(elem(0)))
      }

      list = aggrList.toList
    }

    ("OK",list)
  }


  def AttributesList(groupBy:Any): Option[List[String]] =
  {
    var groupList: Option[List[String]] = None

    if (groupBy == null) {
      //println("list is Null")
      return groupList
    }

    groupBy match
    {
      case groupBy: String =>
        groupBy match{
          case "" => groupList = None //println("groupBy is single string but empty")}
          case _ => {
            var temp: Array[String] = Array(groupBy)
            groupList = Some(temp.toList)
           // println(groupBy)
           // println("groupBy is single string")
            }
        }
      case groupBy: Array[String] => {
        groupList = Some(groupBy.toList)
        //println("groupBy is array string")
      }
    }

    groupList
  }

  def main(args : Array[String]): Unit = {
    /*
    startGMQL()
    val dataPath = "/Users/simone/Downloads/job_filename_guest_new14_20170316_162715_DATA_SET_VAR"
    val r = readDataset(dataPath)

    val input = "NOT(asa<6 AND NOT(asa<6) AND sadas== 'dasd' " +
      "AND asdas==6 AND (adasd=='adsd' AND (a<6 OR b>4) OR c=='c'))"

    val s = select(input,"","",r)
    print(s)*/
  }
}
