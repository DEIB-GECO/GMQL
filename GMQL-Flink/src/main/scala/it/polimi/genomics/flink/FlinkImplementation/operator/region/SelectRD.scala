package it.polimi.genomics.flink.FlinkImplementation.operator.region

import java.io.{File, FileNotFoundException}
import java.nio.file.Paths
import java.util.Locale
import javax.xml.bind.JAXBException

import it.polimi.genomics.core.DataStructures.RegionCondition.REG_OP.REG_OP
import it.polimi.genomics.core.DataStructures.RegionCondition._
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.core.{GDouble, GMQLLoader, GString, GValue}
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import it.polimi.genomics.flink.FlinkImplementation.reader.DataSetFilter
import org.apache.hadoop.fs.{FileSystem, Path}
//import it.polimi.genomics.repository.{Utilities => General_Utilities}
//import it.polimi.genomics.repository.FSRepository.{LFSRepository, Utilities => FSR_Utilities}
import org.apache.flink.api.scala._
import org.apache.flink.hadoop.shaded.com.google.common.base.Charsets
import org.apache.flink.hadoop.shaded.com.google.common.hash.Hashing
import org.apache.hadoop.fs.PathFilter
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._

//{DataSet, ExecutionEnvironment}

/**
 * Created by michelebertoni on 05/05/15.
 */
object SelectRD {

  var executor : FlinkImplementation = null
  final val logger = LoggerFactory.getLogger(this.getClass)
  val locale = Locale.getDefault

  @throws[SelectFormatException]
  def apply(executor : FlinkImplementation, regionCondition : Option[RegionCondition], filteredMeta : Option[MetaOperator], inputDataset : RegionOperator, metaFirst : Boolean, env : ExecutionEnvironment) : DataSet[FlinkRegionType] = {
    this.executor = executor

    val optimized_reg_cond = if (regionCondition.isDefined) Some(optimizeConditionTree(regionCondition.get, false, filteredMeta, env)) else None

    //val input = executor.implement_rd(inputDataset, env)

    val filteredRegion =
      if(filteredMeta.isDefined){

        val metaIdList = executor.implement_md(filteredMeta.get, env)
          .distinct(0).map(_._1)
          .collect
          .toSet
//metaIdList.foreach(println _)
        inputDataset match {
          // Selection from HD
          // SELECTIVE LOAD
          case IRReadRD(paths: List[String], loader: GMQLLoader[Any, Any, Any, Any],_) => {
            val conf = new org.apache.hadoop.conf.Configuration();
            val path = new org.apache.hadoop.fs.Path(paths.head);
            val fs = FileSystem.get(path.toUri(), conf);


            val pathsIn = paths.flatMap((p) => {
              val file = new org.apache.hadoop.fs.Path(p)

//              val fs = FSR_Utilities.getFileSystem
              // if directory, extract all files
              if (fs.isDirectory(file)) {
                fs.listStatus(new org.apache.hadoop.fs.Path(p), new PathFilter {
                  override def accept(path: org.apache.hadoop.fs.Path): Boolean = !path.toString.endsWith(".meta")
                }).map{x=>val path = x.getPath.toString; val p = path.substring(path.indexOf(":")+1, path.size).replaceAll("/","");/*logger.info(p);*/((Hashing.md5().hashString(p, Charsets.UTF_8).asLong()) ->x.getPath.toString)}.toList
              }else {

//                val repository = new LFSRepository()
//                val ds = new IRDataSet(p, List[(String,PARSING_TYPE)]().asJava)
                // if not directory
//                if(repository.DSExists( ds)){
//                  val username = if(repository.DSExistsInPublic(ds)) "public" else General_Utilities().USERNAME
//                  //if repository extract paths from repository
//
//                  val list =
//                    try {
//                      import scala.collection.JavaConverters._
//                      repository.ListDSSamples(p).asScala.map(d =>
//                        if (General_Utilities().MODE.equals("MAPREDUCE")) {
//                          val hdfs = FSR_Utilities.gethdfsConfiguration().get("fs.defaultFS")
//                          hdfs + General_Utilities().getHDFSRegionDir(username) + d.name
//                        }
//                        else { d.name}
//                      )
//                    } catch {
//                      case ex: JAXBException => logger.error("DataSet is corrupted"); List[String]()
//                      case ex: FileNotFoundException => logger.error("DataSet is not Found"); List[String]()
//                    }
//
//                  list.map((subFile) => {
//                    ((Hashing.md5().hashString(subFile.toString.substring(subFile.toString.indexOf(":")+1, subFile.toString.size).replaceAll("/",""), Charsets.UTF_8).asLong()) -> subFile.toString)
//                  })
//
//                } else
                {

                  //else use as file path
                  Array((Hashing.md5().hashString(new Path(p).toString.substring(new Path(p).toString.indexOf(":")+1, new Path(p).toString.size).replaceAll("/",""), Charsets.UTF_8).asLong()) -> p)
                }
              }

            }).toMap

//            println("PATHS = " + pathsIn.map(x=>x/*(x._1,x._2.substring(x._2.indexOf(":")+1, x._2.size))*/).mkString("\n"))
//            println(metaIdList.mkString("\n"))
            val filteredPaths = metaIdList.map((v) => pathsIn.get(v).get).toList
//            println("PATHS = " + filteredPaths.mkString(" - "))

            import scala.collection.JavaConverters._
            executor.implement_rd(new IRReadRD(filteredPaths, loader, IRDataSet("",List[(String,PARSING_TYPE)]().asJava)), env)
            //ReadRD(filteredPaths, loader, metaFirst, env)
          }

            //Selection from memory
          case _ => executor.implement_rd(inputDataset, env).filter((a : FlinkRegionType) => metaIdList.contains(a._1))
        }
      } else {
        executor.implement_rd(inputDataset, env)
      }

    if(optimized_reg_cond.isDefined){
      filteredRegion.filter((region : FlinkRegionType) => {
        applyRegionSelect(optimized_reg_cond.get, region)
      })} else {
      filteredRegion
    }

  }


  def optimizeConditionTree(regionCondition : RegionCondition, not : Boolean, filteredMeta : Option[MetaOperator], env : ExecutionEnvironment) : RegionCondition = {
    regionCondition match {


      case cond : NOT => {
        //optimizeConditionTree(cond.predicate, !not, filteredMeta, env)
        NOT(optimizeConditionTree(cond.predicate, not, filteredMeta, env))
      }

      case cond : OR => {
        OR(optimizeConditionTree(cond.first_predicate, not, filteredMeta, env), optimizeConditionTree(cond.second_predicate, not, filteredMeta, env))
        /*
        if (not) {
          //!OR = AND
          AND(optimizeConditionTree(cond.first_predicate, not, filteredMeta, env), optimizeConditionTree(cond.second_predicate, not, filteredMeta, env))
        } else {
          //OR
          OR(optimizeConditionTree(cond.first_predicate, not, filteredMeta, env), optimizeConditionTree(cond.second_predicate, not, filteredMeta, env))
        }
        */
      }

      case cond : AND => {
        AND(optimizeConditionTree(cond.first_predicate, not, filteredMeta, env), optimizeConditionTree(cond.second_predicate, not, filteredMeta, env))
        /*
        if (not) {
          //!AND = OR
          OR(optimizeConditionTree(cond.first_predicate, not, filteredMeta, env), optimizeConditionTree(cond.second_predicate, not, filteredMeta, env))
        } else {
          //AND
          AND(optimizeConditionTree(cond.first_predicate, not, filteredMeta, env), optimizeConditionTree(cond.second_predicate, not, filteredMeta, env))
        }
        */
      }

      case predicate : Predicate => {

        val value = predicate.value match{

          case v : MetaAccessor => {
            val meta = executor.implement_md(filteredMeta.get, env)
            meta.filter(_._2.equals(v.attribute_name)).distinct.collect
          }

          case v : Any => {
            predicate.value
          }
        }

        predicate.operator match {

          case REG_OP.EQ => {
            Predicate(predicate.position, REG_OP.EQ, value)
            /*
            if (not) {
              //!EQ = NOTEQ
              Predicate(predicate.position, REG_OP.NOTEQ, value)
            } else {
              //EQ
              Predicate(predicate.position, REG_OP.EQ, value)
            }
            */
          }

          case REG_OP.NOTEQ => {
            Predicate(predicate.position, REG_OP.NOTEQ, value)
            /*
            if (not) {
              //!NOTEQ = EQ
              Predicate(predicate.position, REG_OP.EQ, value)
            } else {
              //NOTEQ
              Predicate(predicate.position, REG_OP.NOTEQ, value)
            }
            */
          }

          case REG_OP.GT => {
            Predicate(predicate.position, REG_OP.GT, value)
            /*
            if (not) {
              //!GT = LTE
              Predicate(predicate.position, REG_OP.LTE, value)
            } else {
              //GT
              Predicate(predicate.position, REG_OP.GT, value)
            }
            */
          }

          case REG_OP.GTE => {
            Predicate(predicate.position, REG_OP.GTE, value)
            /*
            if (not) {
              //!GTE = LT
              Predicate(predicate.position, REG_OP.LT, value)
            } else {
              //GTE
              Predicate(predicate.position, REG_OP.GTE, value)
            }
            */
          }

          case REG_OP.LT => {
            Predicate(predicate.position, REG_OP.LT, value)
            /*
            if (not) {
              //!LT = GTE
              Predicate(predicate.position, REG_OP.GTE, value)
            } else {
              //LT
              Predicate(predicate.position, REG_OP.LT, value)
            }
            */
          }

          case REG_OP.LTE => {
            Predicate(predicate.position, REG_OP.LTE, value)
            /*
            if (not) {
              //!LTE = GT
              Predicate(predicate.position, REG_OP.GT, value)
            } else {
              //LTE
              Predicate(predicate.position, REG_OP.LTE, value)
            }
            */
          }
        }
      }

      case noNeedForOptimization : RegionCondition => {
        noNeedForOptimization
      }
    }
  }




  @throws[SelectFormatException]
  def applyRegionSelect(regionCondition: RegionCondition, input: FlinkRegionType) : Boolean = {
    regionCondition match {

      case chrCond : ChrCondition => {
        input._2.toLowerCase(locale).equals(chrCond.chr_name.toLowerCase(locale))
      }

      case strandCond : StrandCondition => {
        input._5.equals(strandCond.strand.charAt(0))
      }

      case leftEndCond : LeftEndCondition => {
        applyRegionPredicate(leftEndCond.op, leftEndCond.value, GDouble(input._3), input._1)
      }

      case rightEndCond : RightEndCondition => {
        applyRegionPredicate(rightEndCond.op, rightEndCond.value, GDouble(input._4), input._1)
      }

      case startCond : StartCondition => {
        input._5 match {
          case '*' =>
            applyRegionPredicate(startCond.op, startCond.value, GDouble(input._3), input._1)
          case '+' =>
            applyRegionPredicate(startCond.op, startCond.value, GDouble(input._3), input._1)
          case '-' =>
            applyRegionPredicate(startCond.op, startCond.value, GDouble(input._4), input._1)
        }
      }

      case stopCond : StopCondition => {
        input._5 match {
          case '*' =>
            applyRegionPredicate(stopCond.op, stopCond.value, GDouble(input._4), input._1)
          case '+' =>
            applyRegionPredicate(stopCond.op, stopCond.value, GDouble(input._4), input._1)
          case '-' =>
            applyRegionPredicate(stopCond.op, stopCond.value, GDouble(input._3), input._1)
        }
      }

      case predicate: RegionCondition.Predicate => {
        applyRegionPredicate(predicate.operator, predicate.value, input._6(predicate.position), input._1)
      }

      case region_cond: OR => {
        applyRegionConditionOR(region_cond.first_predicate, region_cond.second_predicate, input)
      }

      case region_cond: AND => {
        applyRegionConditionAND(region_cond.first_predicate, region_cond.second_predicate, input)
      }

      case region_cond: NOT => {
        applyRegionConditionNOT(region_cond.predicate, input)
      }
    }
  }

  def applyRegionPredicate(operator : REG_OP, value : Any, input : GValue, sampleID : Long) : Boolean ={

    operator match {

      case REG_OP.EQ => {
        applyRegionPredicateEQ(value, input, sampleID)
      }

      case REG_OP.NOTEQ => {
        applyRegionPredicateNOTEQ(value, input, sampleID)
      }

      case REG_OP.GT => {
        applyRegionPredicateGT(value, input, sampleID)
      }

      case REG_OP.GTE => {
        applyRegionPredicateGTE(value, input, sampleID)
      }

      case REG_OP.LT => {
        applyRegionPredicateLT(value, input, sampleID)
      }

      case REG_OP.LTE => {
        applyRegionPredicateLTE(value, input, sampleID)
      }
    }
  }




  //Predicate evaluation methods

  @throws[SelectFormatException]
  def applyRegionPredicateEQ(value : Any, input : GValue, sampleID : Long) : Boolean = {
    //println("--------- debug 1 " + input + "--------- debug 2 " + value)
    value match {
      case metaList : Seq[FlinkMetaType] => {
        try{
          applyRegionPredicateEQ(castDoubleOrString(metaList.filter(_._1.equals(sampleID)).head._3), input, sampleID)
        } catch {
          case e: IndexOutOfBoundsException => false
        }
      }

      case value : Int => {
        input.asInstanceOf[GDouble].v.equals(value.toDouble)
      }

      case value : Long => {
        input.asInstanceOf[GDouble].v.equals(value.toDouble)
      }

      case value : Double => {
        input.asInstanceOf[GDouble].v.equals(value.toDouble)
      }

      case value : String => {
        input.asInstanceOf[GString].v.toLowerCase(locale).equals(value.toLowerCase(locale))
      }
    }
  }

  @throws[SelectFormatException]
  def applyRegionPredicateNOTEQ(value : Any, input : GValue, sampleID : Long): Boolean = {
    value match {

      case metaList : Seq[FlinkMetaType] => {
        try {
          applyRegionPredicateNOTEQ(castDoubleOrString(metaList.filter(_._1.equals(sampleID)).head._3), input, sampleID)
        } catch {
          case e: IndexOutOfBoundsException => false
        }
      }

      case value : Int => {
        !input.asInstanceOf[GDouble].v.equals(value.toDouble)
      }

      case value : Long => {
        input.asInstanceOf[GDouble].v.equals(value.toDouble)
      }

      case value : Double => {
        !input.asInstanceOf[GDouble].v.equals(value.toDouble)
      }

      case value : String => {
        !input.asInstanceOf[GString].v.toLowerCase(locale).equals(value.toLowerCase(locale))
      }
    }
  }

  @throws[SelectFormatException]
  def applyRegionPredicateLT(value : Any, input : GValue, sampleID : Long): Boolean = {
    value match {
      case metaList : Seq[FlinkMetaType] => {
        try {
          applyRegionPredicateLT(castDoubleOrString(metaList.filter(_._1.equals(sampleID)).head._3), input, sampleID)
        } catch {
          case e : IndexOutOfBoundsException => false
        }
      }

      case value : Int => {
        input.asInstanceOf[GDouble].v < value.toDouble
      }

      case value : Long => {
        input.asInstanceOf[GDouble].v < value.toDouble
      }

      case value : Double => {
        input.asInstanceOf[GDouble].v < value.toDouble
      }

      case value : String => {
        throw SelectFormatException.create("Your SELECT statement cannot be executed: yuo are doing a < comparison between string. Query: " + value + " < " + input)
      }
    }
  }

  @throws[SelectFormatException]
  def applyRegionPredicateLTE(value : Any, input : GValue, sampleID : Long): Boolean = {
    value match {
      case metaList : Seq[FlinkMetaType] => {
        try {
          applyRegionPredicateLTE(castDoubleOrString(metaList.filter(_._1.equals(sampleID)).head._3), input, sampleID)
        } catch {
          case e : IndexOutOfBoundsException => false
        }
      }

      case value : Int => {
        input.asInstanceOf[GDouble].v <= value.toDouble
      }

      case value : Long => {
        input.asInstanceOf[GDouble].v <= value.toDouble
      }

      case value : Double => {
        input.asInstanceOf[GDouble].v <= value.toDouble
      }

      case value : String => {
        throw SelectFormatException.create("Your SELECT statement cannot be executed: yuo are doing a <= comparison between string. Query: " + value + " <= " + input)
      }
    }
  }

  @throws[SelectFormatException]
  def applyRegionPredicateGT(value : Any, input : GValue, sampleID : Long): Boolean = {
    value match {
      case metaList : Seq[FlinkMetaType] => {
        try{
          applyRegionPredicateGT(castDoubleOrString(metaList.filter(_._1.equals(sampleID)).head._3), input, sampleID)
        } catch {
          case e: IndexOutOfBoundsException => false
        }
      }

      case value : Int => {
        input.asInstanceOf[GDouble].v > value.toDouble
      }

      case value : Long => {
        input.asInstanceOf[GDouble].v > value.toDouble
      }

      case value : Double => {
        input.asInstanceOf[GDouble].v > value.toDouble
      }

      case value : String => {
        throw SelectFormatException.create("Your SELECT statement cannot be executed: yuo are doing a > comparison between string. Query: " + value + " > " + input)
      }
    }
  }

  @throws[SelectFormatException]
  def applyRegionPredicateGTE(value : Any, input : GValue, sampleID : Long): Boolean = {
    value match {
      case metaList : Seq[FlinkMetaType] => {
        try{
          applyRegionPredicateGTE(castDoubleOrString(metaList.filter(_._1.equals(sampleID)).head._3), input, sampleID)
        } catch {
          case e: IndexOutOfBoundsException => false
        }
      }

      case value : Int => {
        input.asInstanceOf[GDouble].v >= value.toDouble
      }

      case value : Long => {
        input.asInstanceOf[GDouble].v >= value.toDouble
      }

      case value : Double => {
        input.asInstanceOf[GDouble].v >= value.toDouble
      }

      case value : String => {
        throw SelectFormatException.create("Your SELECT statement cannot be executed: yuo are doing a >= comparison between string. Query: " + value + " >= " + input)
      }
    }
  }




  //Composed metacondition evaluation methods

  @throws[SelectFormatException]
  def applyRegionConditionOR(region1: RegionCondition, region2: RegionCondition, input: FlinkRegionType) : Boolean = {
    applyRegionSelect(region1, input) || applyRegionSelect(region2, input)
  }

  @throws[SelectFormatException]
  def applyRegionConditionAND(region1: RegionCondition, region2: RegionCondition, input: FlinkRegionType) : Boolean = {
    applyRegionSelect(region1, input) && applyRegionSelect(region2, input)
  }

  @throws[SelectFormatException]
  def applyRegionConditionNOT(regions: RegionCondition, input: FlinkRegionType) : Boolean = {
    !applyRegionSelect(regions, input)
  }



  //Other useful methods

  /*
  def castIntDoubleString(a:Any) = {
    try{
      a.toString.toInt
    } catch {
      case e : Throwable => {
        try{
          a.toString.toDouble
        } catch {
          case e : Throwable => {
            a.toString
          }
        }
      }
    }
  }
  */

  def castDoubleOrString(value : Any) = {
    try{
      value.toString.toDouble
    } catch {
      case e : Throwable => value.toString.toLowerCase(locale)
    }
  }

}
