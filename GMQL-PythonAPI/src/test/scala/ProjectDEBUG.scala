import java.util

import it.polimi.genomics.core.DataStructures.RegionAggregate.RegionFunction
import it.polimi.genomics.pythonapi.PythonManager

/**
  * Created by Luca Nanni on 19/06/17.
  * Email: luca.nanni@mail.polimi.it
  */
object ProjectDEBUG {

  def main(args: Array[String]): Unit = {

    val inputPath = "/home/luca/Documenti/resources/hg_narrowPeaks_short"
    PythonManager.startEngine()
    val index = PythonManager.read_dataset(inputPath, PythonManager.getParser("NarrowPeakParser"))

    val operatorManager = PythonManager.getOperatorManager
    val expressionBuilder = PythonManager.getNewExpressionBuilder(index)

    val projected_regs = new java.util.ArrayList[String]()
    projected_regs.add("pValue")
    val extended_regs = expressionBuilder.createRegionExtension("start",
      expressionBuilder.getBinaryRegionExpression(
        expressionBuilder.getRENode("start"), "ADD", expressionBuilder.getREType("integer", "1")
      )
    )
    val extended_regs_list = new util.ArrayList[RegionFunction]()
    extended_regs_list.add(extended_regs)
    val new_index = operatorManager.project(index, None, None, Some(projected_regs),None, Some(extended_regs_list))

    val result = PythonManager.collect(new_index)
  }

}
