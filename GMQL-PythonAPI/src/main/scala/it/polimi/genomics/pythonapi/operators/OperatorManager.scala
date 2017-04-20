package it.polimi.genomics.pythonapi.operators

import it.polimi.genomics.core.DataStructures.MetaAggregate.MetaAggregateStruct
import it.polimi.genomics.core.DataStructures.MetadataCondition.MetadataCondition
import it.polimi.genomics.core.DataStructures.RegionAggregate.RegionFunction
import it.polimi.genomics.core.DataStructures.RegionCondition.RegionCondition
import it.polimi.genomics.pythonapi.PythonManager

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
  * Created by Luca Nanni on 11/04/17.
  * Email: luca.nanni@mail.polimi.it
  */
object OperatorManager {

  def meta_select(index: Int, metaCondition : MetadataCondition) : Int =
  {
    // get the corresponding variable
    val v = PythonManager.getVariable(index)
    // do the operation (build the DAG)
    val nv = v SELECT metaCondition
    // generate new index
    val new_index = PythonManager.putNewVariable(nv)
    new_index
  }

  def reg_select(index: Int, regionCondition: RegionCondition) : Int =
  {
    // get the corresponding variable
    val v = PythonManager.getVariable(index)
    // do the operation (build the DAG)
    val nv = v SELECT regionCondition
    // generate new index
    val new_index = PythonManager.putNewVariable(nv)
    new_index
  }

  def select(index: Int, metaCondition : MetadataCondition, regionCondition: RegionCondition) : Int = ???

  def meta_project(index : Int, projected_meta: java.util.List[String]): Int = {
    do_project(index, Option(projected_meta), None, None, None)
  }

  def meta_project_extend(index : Int, projected_meta: java.util.List[String],
                          extended_meta : MetaAggregateStruct): Int = {
    do_project(index, Option(projected_meta), Option(extended_meta), None, None)
  }

  def reg_project(index : Int, projected_regs : java.util.List[String]): Int = {
    do_project(index, None, None, Option(projected_regs), None)
  }

  def reg_project_extend(index : Int, projected_regs : java.util.List[String],
                         extended_regs : java.util.List[RegionFunction]): Int = {
    do_project(index, None, None, Option(projected_regs), Option(extended_regs))
  }

  def do_project(index : Int, projected_meta : Option[java.util.List[String]], extended_meta : Option[MetaAggregateStruct],
                 projected_regs : Option[java.util.List[String]], extended_regs : Option[java.util.List[RegionFunction]]): Int =
  {
    // get the corresponding variable
    val v = PythonManager.getVariable(index)

    var projected_meta_values : Option[List[String]] = None
    if(projected_meta.isDefined) {
      projected_meta_values = Option(projected_meta.get.asScala.toList)
    }

    var projected_regs_values : Option[List[Int]] = None
    if(projected_regs.isDefined) {
      val l = projected_regs.get.asScala.toList
      val proj = new ListBuffer[Int]()
      for(p <- l) {
        val n = v.get_field_by_name(p)
        if(n.isDefined){
          proj += n.get
        }
      }
      projected_regs_values = Option(proj.toList)
    }

    var extended_regs_l : Option[List[RegionFunction]] = None
    if(extended_regs.isDefined) {
      val extnd = new ListBuffer[RegionFunction]()
      val l = extended_regs.get.asScala.toList
      for(r <- l) {
        extnd += r
      }
      extended_regs_l = Option(extnd.toList)
    }


    // do the operation
    val nv = v PROJECT(projected_meta_values,
      extended_meta, projected_regs_values,
      extended_regs_l)

    // generate new index
    val new_index = PythonManager.putNewVariable(nv)
    new_index
  }

}
