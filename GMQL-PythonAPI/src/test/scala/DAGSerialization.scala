import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

import Project.properties
import it.polimi.genomics.core.DataStructures.IRVariable
import it.polimi.genomics.pythonapi.{AppProperties, PythonManager}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Created by Luca Nanni on 14/05/17.
  * Email: luca.nanni@mail.polimi.it
  */
object DAGSerialization {

  val logger  = LoggerFactory.getLogger(this.getClass)
  val properties = AppProperties
  // path from where to take the data
  val inputPath = "/home/luca/Documenti/resources/hg_narrowPeaks"

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

    /*
    * Operations on the dataset
    * */

    var index = pythonManager.read_dataset(inputPath, pythonManager.getParser("NarrowPeakParser"))

    val opManager = pythonManager.getOperatorManager

    // FIRST OPERATION: select on metadata
    var expBuilder = pythonManager.getNewExpressionBuilder(index)
    val metaCondition = expBuilder.createMetaBinaryCondition(
      expBuilder.createMetaPredicate("cell","EQ","K562"),
      "AND",
      expBuilder.createMetaPredicate("antibody", "EQ","H3K4me3")
    )
    index = opManager.meta_select(index, -1,  metaCondition, None)

    // SECOND OPERATION: select on region data
    expBuilder = pythonManager.getNewExpressionBuilder(index)
    val regionCondition = expBuilder.createRegionBinaryCondition(
      expBuilder.createRegionBinaryCondition(
        expBuilder.createRegionPredicate("chr","EQ","chr9"),
        "AND",
        expBuilder.createRegionPredicate("start", "GTE","138680")
      )
      , "AND",
      expBuilder.createRegionPredicate("stop", "LTE", "145000")

    )

    index = opManager.reg_select(index, -1, regionCondition, None)


    /*
    * SERIALIZATION
    * */
    val variable : IRVariable = pythonManager.getVariable(index)

    val oos = new ObjectOutputStream(new FileOutputStream("./dag"))

    oos.writeObject(variable)
    oos.close()

    /*
    * DESERIALIZATION
    * */

    val ois = new ObjectInputStream(new FileInputStream("./dag"))
    val des_variable = ois.readObject.asInstanceOf[IRVariable]
    println("FINISH")
  }

}
