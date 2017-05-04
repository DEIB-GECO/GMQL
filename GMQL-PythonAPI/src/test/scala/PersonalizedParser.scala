import MultipleMaterializations.properties
import it.polimi.genomics.pythonapi.{AppProperties, PythonManager}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Created by Luca Nanni on 29/04/17.
  * Email: luca.nanni@mail.polimi.it
  */
object PersonalizedParser {
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
  }
}
