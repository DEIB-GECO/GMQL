import java.util

import MultipleMaterializations.properties
import it.polimi.genomics.core.DataStructures.RegionAggregate.RegionFunction
import it.polimi.genomics.pythonapi.{AppProperties, PythonManager}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Created by Luca Nanni on 20/04/17.
  * Email: luca.nanni@mail.polimi.it
  */
object Project {

  val logger  = LoggerFactory.getLogger(this.getClass)
  val properties = AppProperties

  def main(args: Array[String]): Unit = {
    /*
     * Setting up the Spark context
     * */
    val conf = new SparkConf()
      .setAppName(properties.applicationName)
      .setMaster(properties.master)
      .set("spark.serializer", properties.serializer)

    val sc = new SparkContext(conf)
    this.logger.info("Spark context initiated")

    val pythonManager = PythonManager
    pythonManager.setSparkContext(sc=sc)

    // start engine
    pythonManager.startEngine()

    // path from where to take the data
    val inputPath = "/home/luca/Documenti/resources/hg_narrowPeaks"

    val opManager = pythonManager.getOperatorManager

    // read the data
    val index = pythonManager.read_dataset(dataset_path = inputPath, parserName = "NarrowPeakParser")

    val regFields = new java.util.ArrayList[String]()
    regFields.add("score")
    regFields.add("signalValue")
    regFields.add("pValue")

    //build new fields
    val expBuild = pythonManager.getNewExpressionBuilder(index)
    val regExtendedFields = new util.ArrayList[RegionFunction]()
    val reFun = expBuild.createRegionExtension(
      "new_attribute", expBuild.getBinaryRegionExpression(expBuild.getRENode("pValue"), "ADD", expBuild.getRENode("signalValue"))
    )
    regExtendedFields.add(reFun)

    val new_index = opManager.reg_project_extend(index,regFields,regExtendedFields)
  }

}
