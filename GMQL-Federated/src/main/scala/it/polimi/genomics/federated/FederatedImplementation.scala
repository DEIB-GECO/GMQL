package it.polimi.genomics.federated

import it.polimi.genomics.GMQLServer.Implementation
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.{GMQLLoaderBase, GMQLSchemaCoordinateSystem, GMQLSchemaFormat}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Stack

trait FederatedStep
case class Move(from: Instance, to: Instance) extends FederatedStep
case class LocalExecute(iRVariable: IRVariable) extends FederatedStep
case class RemoteExecute(iRVariable: IRVariable, instance: Instance) extends FederatedStep

class FederatedImplementation extends Implementation with Serializable{

  val conf = new SparkConf().setAppName("GMQL V2.1 Spark " )
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryoserializer.buffer", "128")
    .set("spark.driver.allowMultipleContexts","true")
    .set("spark.sql.tungsten.enabled", "true").setMaster("local[*]")
  val sc: SparkContext = new SparkContext(conf)

  val gse = new GMQLSparkExecutor(
    testingIOFormats = false,
    sc = sc,
    outputFormat = GMQLSchemaFormat.VCF,
    outputCoordinateSystem = GMQLSchemaCoordinateSystem.Default)



  /** Starts the execution */
  override def go(): Unit = {
    println("####  Federated #####" )
    implementation()
  }

  override def collect(iRVariable: IRVariable): Any = println("Collect")

  override def take(iRVariable: IRVariable, n: Int): Any = println("take")

  /** stop GMQL implementation (kill a job) */
  override def stop(): Unit = println("stop")

  /** given the name of a parser, returns it. It must at least provide a parser for the "default" name */
  override def getParser(name: String, dataset: String): GMQLLoaderBase = {
    println("getParser")
    gse.getParser(name,dataset)
  }


  def implementation(): Unit = {


    def getLocation(iROperator: IROperator) = {
      iROperator
        .annotations
        .find(_.isInstanceOf[EXECUTED_ON])
        .map(_.asInstanceOf[EXECUTED_ON])
        .getOrElse(EXECUTED_ON(LOCAL_INSTANCE))
    }

    def getSubDags(metadag: IROperator, regiondag: IROperator) : List[(IROperator, IROperator)] = {

      val metaAddress =  getLocation(metadag)
      val regAddress =  getLocation(regiondag)

      var dag = List.empty[IROperator]
      if (metaAddress == regAddress) {

      }

      List.empty
    }

    val stack = Stack[FederatedStep]()
        for (variable <- to_be_materialized) {

          val metaAddress =  getLocation(variable.metaDag)
          val regAddress =  getLocation(variable.regionDag)




          if (metaAddress != regAddress) {
            println("ERRORE")
            sys.exit(0)
          }
          println(variable.metaDag)
          println(variable.regionDag)
      }
  }
}


