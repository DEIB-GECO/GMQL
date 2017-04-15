package it.polimi.genomics.pythonapi.operators

import it.polimi.genomics.core.DataStructures.IRVariable
import it.polimi.genomics.core.DataStructures.MetadataCondition.MetadataCondition
import it.polimi.genomics.core.DataStructures.RegionCondition.RegionCondition
import it.polimi.genomics.pythonapi.PythonManager

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

}
