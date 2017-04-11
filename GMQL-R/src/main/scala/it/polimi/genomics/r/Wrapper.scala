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
  var sparkContext:SparkContext = _
  var GMQLServer: GmqlServer = _
  var parser = new CustomParser()

  //thread safe counter for unique string pointer to dataset
  //'cause we could have two pointer at the same dataset and we
  // can not distinguesh them
  //example:
  // R shell
  //r = readDataset("/Users/simone/Downloads/job_filename_guest_new14_20170316_162715_DATA_SET_VAR")
  //r1 = readDataset("/Users/simone/Downloads/job_filename_guest_new14_20170316_162715_DATA_SET_VAR")
  //are mapped in vv with the same key
  var counter: AtomicLong = new AtomicLong(0)

  var vv: Map[String,IRVariable] = Map[String,IRVariable]()

  def SetSparkContext(): Unit =
  {
    var sparkConf = new SparkConf().setMaster("local[*]").setAppName("GMQL-R")
    sparkContext = new SparkContext(sparkConf)
    println("Spark Context up")
  }

  def runGMQLServer(): Unit  =
  {
    if(sparkContext==null) {
      println("you must initialize sparkContext first")
      return
    }
    GMQLServer = new GmqlServer(new GMQLSparkExecutor(sc=sparkContext))
    println("Gmql server up")
  }

  //debug
  def stopGMQL(): Unit =
  {
    sparkContext.stop()
  }

  def readDataset(data_input_path: String): String =
  {
    val dataPath  = data_input_path+ "/files"
    parser.setSchema(dataPath)
    val dataAsTheyAre = GMQLServer.READ(dataPath).USING(parser)

    var index = counter.getAndIncrement()
    val out_p = "dataset"+index
    vv = vv + (out_p -> dataAsTheyAre)

    return out_p
  }

  def materialize(data_to_materialize: String, data_output_path: String): Unit =
  {
    val materialize = vv.get(data_to_materialize).get

    GMQLServer setOutputPath data_output_path MATERIALIZE materialize
    GMQLServer.run()
  }

  def select(predicate:String,region:String,semijoin:Any, input_dataset: String): String =
  {
    val dataAsTheyAre = vv.get(input_dataset).get

    val meta_con =
      DataStructures.MetadataCondition.AND(
        DataStructures.MetadataCondition.Predicate("cell",DataStructures.MetadataCondition.META_OP.GTE, "11"),
        DataStructures.MetadataCondition.NOT(
          DataStructures.MetadataCondition.Predicate("provider", DataStructures.MetadataCondition.META_OP.NOTEQ, "UCSC")
        )
      )

    val   reg_con =
    //        DataStructures.RegionCondition.OR(
    //          DataStructures.RegionCondition.Predicate(3, DataStructures.RegionCondition.REG_OP.GT, 30),
    //DataStructures.RegionCondition.Predicate(0, DataStructures.RegionCondition.REG_OP.EQ, "400")
      DataStructures.RegionCondition.Predicate(2, DataStructures.RegionCondition.REG_OP.GT, DataStructures.RegionCondition.MetaAccessor("cell"))
    //        )

    val select = dataAsTheyAre.SELECT(meta_con,reg_con)
    val out_p = input_dataset+"/select"
    vv = vv + (out_p -> select )

    return out_p
    //GMQLServer setOutputPath data_output_path MATERIALIZE select
    //GMQLServer.run()
  }

  def project(data_input_path: String, data_output_path: String): Unit =
  {
    /*
    parser.setSchema(data_input_path)
    val dataAsTheyAre = GMQLServer.READ(data_input_path).USING(parser)

    val project = dataAsTheyAre.PROJECT()
    //GMQLServer setOutputPath data_output_path MATERIALIZE project
    //GMQLServer.run()
    */
  }

  def extend(metadata:List[Array[String]], input_dataset: String): String =
  {
    val dataAsTheyAre = vv.get(input_dataset).get
    var metaList = RegionToMetaAggregates(metadata,dataAsTheyAre)
    var extend = dataAsTheyAre.EXTEND(metaList)

    val out_p = input_dataset+"/extend"
    vv = vv + (out_p -> extend )
    return out_p
  }

  def group(groupBy:Any, data_input_path: String, data_output_path: String): Unit =
  {
    /*
    parser.setSchema(data_input_path)
    val dataAsTheyAre = GMQLServer.READ(data_input_path).USING(parser)

    var groupList: Option[List[String]] = GroupByList(groupBy)
    if (groupList == null)
      return

    //var group = dataAsTheyAre.GROUP()
    //GMQLServer setOutputPath data_output_path MATERIALIZE group
    //GMQLServer.run()
    */
  }


  def merge(groupBy:Any, input_dataset: String): Unit =
  {
    val dataAsTheyAre = vv.get(input_dataset).get

    var groupList: Option[List[String]] = GroupByList(groupBy)
    var merge = dataAsTheyAre.MERGE(groupList)

    val out_p = input_dataset+"/merge"
    vv = vv + (out_p -> merge )
    return out_p

    //GMQLServer setOutputPath data_output_path MATERIALIZE merge
    //GMQLServer.run()
  }

  def order(data_input_path: String, data_output_path: String): Unit =
  {
    /*
    parser.setSchema(data_input_path)
    val dataAsTheyAre = GMQLServer.READ(data_input_path).USING(parser)
*/

    //GMQLServer setOutputPath data_output_path MATERIALIZE select
    //GMQLServer.run()
  }

  def union(right_dataset: String, right_name: String,
            left_dataset: String, left_name: String): Unit =
  {
    val rightDataAsTheyAre = vv.get(right_dataset).get
    val leftDataAsTheyAre = vv.get(left_dataset).get

    val union = leftDataAsTheyAre.UNION(rightDataAsTheyAre,left_name,right_name)

    val out_p = left_dataset+right_dataset+"/union"
    vv = vv + (out_p -> union )
    return out_p

    //GMQLServer setOutputPath data_output_path MATERIALIZE union
    //GMQLServer.run()
  }

  //TODO: implement joinBy
  def difference(joinBy:List[Array[String]],left_dataset: String,right_dataset: String): Unit =
  {
    val leftDataAsTheyAre = vv.get(left_dataset).get
    val rightDataAsTheyAre = vv.get(right_dataset).get

    val difference = leftDataAsTheyAre.DIFFERENCE(None,rightDataAsTheyAre)
    val out_p = left_dataset+right_dataset+"/difference"
    vv = vv + (out_p -> difference )
    return out_p

    //GMQLServer setOutputPath data_output_path MATERIALIZE difference
    //GMQLServer.run()
  }

  def flat(min:Int , max:Int, groupBy:Any,aggregates:List[Array[String]], input_dataset: String): String ={
    return "flat"//return selectedVariant(CoverFlag.FLAT,min,max,groupBy,aggregates,input_dataset)
  }

  def histogram(min:Int , max:Int, groupBy:Any,aggregates:List[Array[String]], input_dataset: String): String = {
    return "histogram"//return selectedVariant(CoverFlag.HISTOGRAM,min,max,groupBy,aggregates,input_dataset)
  }

  def summit(min:Int , max:Int, groupBy:Any,aggregates:List[Array[String]], input_dataset: String): String ={
    return "summit"//return selectedVariant(CoverFlag.SUMMIT,min,max,groupBy,aggregates,input_dataset)
  }

  def cover(min:Int , max:Int, groupBy:Any,aggregates:List[Array[String]], input_dataset: String): String =  {
    return "cover"//return selectedVariant(CoverFlag.COVER,min,max,groupBy,aggregates,input_dataset)
  }

  def selectedVariant(flag:CoverFlag.CoverFlag, min:Int , max:Int, groupBy:Any,aggregates:List[Array[String]], input_dataset: String): String =
  {
    val dataAsTheyAre = vv.get(input_dataset).get
    var paramMin:CoverParam = null
    var paramMax:CoverParam = null

    min match {
      case 0 => paramMin = ANY()
      case -1 => paramMin = ALL()
      case x if x > 0 => paramMin = N(min.toInt)
    }

    max match {
      case 0 => paramMax = ANY()
      case -1 => paramMax = ALL()
      case x if x > 0 => paramMax = N(max.toInt)
    }

    var aggrlist = RegionToRegionAggregates(aggregates,dataAsTheyAre)
    var groupList: Option[List[String]] = GroupByList(groupBy)

    var cover = dataAsTheyAre.COVER(flag,paramMin, paramMax, aggrlist , groupList)

    flag match
    {
      case CoverFlag.COVER => vv = vv + (input_dataset+"/cover" -> cover ); return input_dataset+"/cover"
      case CoverFlag.SUMMIT => vv = vv + (input_dataset+"/summit" -> cover ); return input_dataset+"/summit"
      case CoverFlag.FLAT => vv = vv + (input_dataset+"/flat" -> cover ); return input_dataset+"/flat"
      case CoverFlag.HISTOGRAM => vv = vv + (input_dataset+"/histogram" -> cover ); return input_dataset+"/histogram"
    }

    //GMQLServer setOutputPath data_output_path MATERIALIZE cover
    //GMQLServer.run()
  }

  def map(aggregates:List[Array[String]],right_data_input_path: String, exp_name: String,
          left_data_input_path: String, ref_name: String,count_name: String): Unit =
  {
    parser.setSchema(left_data_input_path)
    val leftDataAsTheyAre = GMQLServer.READ(left_data_input_path).USING(parser)

    parser.setSchema(right_data_input_path)
    val rightDataAsTheyAre = GMQLServer.READ(right_data_input_path).USING(parser)

    var aggrlist = RegionToRegionAggregates(aggregates,leftDataAsTheyAre)
    if (aggrlist == null)
      return

    val map = leftDataAsTheyAre.MAP(None,aggrlist,rightDataAsTheyAre,Option(ref_name),Option(exp_name),Option(count_name))
    //GMQLServer setOutputPath data_output_path MATERIALIZE union
    //GMQLServer.run()
  }

  def join():Unit =
  {

  }


/*  Aggregates is structured in R as: c("nuovoP","SUM","pvalue") that represent an array.
the first elem is the output name, the second the aggregate function and the third
is the value present in the schema
*/
  def RegionToMetaAggregates(aggregates:List[Array[String]],data:IRVariable): List[RegionsToMeta] =
  {
    var list:List[RegionsToMeta] = List()
    if (aggregates == null)
      return list

    val aggrList =  new ListBuffer[RegionsToMeta]()

    for (elem <- aggregates) {
      if (elem(1).equalsIgnoreCase("COUNT"))
        aggrList += DefaultRegionsToMetaFactory.get(elem(1), Some(elem(0)))
      else
      {
        val field = data.get_field_by_name(elem(2))
        if(field.isEmpty)
          return null
        aggrList += DefaultRegionsToMetaFactory.get(elem(1), field.get, Some(elem(0)))
      }

      list = aggrList.toList
    }

    return list
  }

  def RegionToRegionAggregates(aggregates:List[Array[String]],data:IRVariable): List[RegionsToRegion] =
  {
    var list:List[RegionsToRegion] = List()
    if (aggregates == null)
      return list

    val aggrList =  new ListBuffer[RegionsToRegion]()

    for (elem <- aggregates) {
        if (elem(1).equalsIgnoreCase("COUNT"))
          aggrList += DefaultRegionsToRegionFactory.get(elem(1), Some(elem(0)))
        else
        {
          val field = data.get_field_by_name(elem(2))
          if(field.isEmpty)
            return null
          aggrList += DefaultRegionsToRegionFactory.get(elem(1), field.get, Some(elem(0)))
      }

      list = aggrList.toList
    }

    return list
  }


  def GroupByList(groupBy:Any): Option[List[String]] =
  {
    var groupList: Option[List[String]] = None

    if (groupBy == null)
    {
      println("groupBy is Null")
      return groupList
    }

    groupBy match
    {
      case groupBy: String =>
      {
        groupBy match{
          case "" => {groupList = None; println("groupBy is single string but empty")}
          case _ => {
            var temp: Array[String] = Array(groupBy)
            groupList = Some(temp.toList)
            println(groupBy)
            println("groupBy is single string")}
        }
      }
      case groupBy: Array[String] => {
        groupList = Some(groupBy.toList)
        println("groupBy is array string")
      }
      //case groupBy: Array[Array[String]] => {print("Data structure not supported yet, null"); return null}
    }

    return  groupList
  }

  def main(args : Array[String]): Unit = {

    SetSparkContext()
    runGMQLServer()
    var r = readDataset("/Users/simone/Downloads/job_filename_guest_new14_20170316_162715_DATA_SET_VAR")
    var c = cover(2,3,null,null,r)
    materialize(c,"/Users/simone/Downloads/")
  }
}
