import it.polimi.genomics.pythonapi.{AppProperties, PythonManager}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Created by Luca Nanni on 18/04/17.
  * Email: luca.nanni@mail.polimi.it
  */
object MultipleMaterializations {

  val logger  = LoggerFactory.getLogger(this.getClass)
  val properties = AppProperties

  def main(args: Array[String]): Unit = {

    PythonManager.startEngine()

    // path from where to take the data
    val inputPath = "/home/luca/Documenti/resources/hg_narrowPeaks"
    // path where to save the result of the first materialization
    val outputPath_1 = "/home/luca/Documenti/resources/result1"
    // path where to save the result of the second materialization
    val outputPath_2 = "/home/luca/Documenti/resources/result2"

    val opManager = PythonManager.getOperatorManager

    // read the data
    val index = PythonManager.read_dataset(dataset_path = inputPath,
      parserName = "NarrowPeakParser")

    // FIRST OPERATION: select on metadata
    var expBuilder = PythonManager.getNewExpressionBuilder(index)
    val metaCondition = expBuilder.createMetaBinaryCondition(
            expBuilder.createMetaPredicate("cell","EQ","K562"),
            "AND",
            expBuilder.createMetaPredicate("antibody", "EQ","H3K4me3")
    )
    val index1 = opManager.meta_select(index, -1, metaCondition, None)
    // first materialization
    PythonManager.materialize(index1,outputPath_1)
//
//    // SECOND OPERATION: select on region data
//    expBuilder = pythonManager.getNewExpressionBuilder(index)
//    val regionCondition = expBuilder.createRegionBinaryCondition(
//      expBuilder.createRegionBinaryCondition(
//        expBuilder.createRegionPredicate("chr","EQ","chr9"),
//        "AND",
//        expBuilder.createRegionPredicate("start", "GTE","138680")
//      )
//      , "AND",
//      expBuilder.createRegionPredicate("stop", "LTE", "145000")
//
//    )
//
//    val index2 = opManager.reg_select(index, -1, regionCondition, None)
//    // second materialization
//    pythonManager.materialize(index2, outputPath_2)


  }
}
