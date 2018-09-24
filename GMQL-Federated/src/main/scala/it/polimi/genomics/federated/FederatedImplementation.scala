package it.polimi.genomics.federated

import java.util.concurrent.atomic.AtomicInteger

import it.polimi.genomics.GMQLServer.Implementation
import it.polimi.genomics.core.DAG.{DAGWrapper, OperatorDAG, OperatorDAGFrame}
import it.polimi.genomics.core.DataStructures.{IROperator, _}
import it.polimi.genomics.core.{GMQLLoaderBase, GMQLSchemaCoordinateSystem, GMQLSchemaFormat}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Stack}

trait FederatedStep

case class Move(from: Instance, to: Instance) extends FederatedStep

case class LocalExecute(iRVariable: IRVariable) extends FederatedStep

case class RemoteExecute(iRVariable: IRVariable, instance: Instance) extends FederatedStep

class FederatedImplementation extends Implementation with Serializable {

  val conf = new SparkConf().setAppName("GMQL V2.1 Spark ")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryoserializer.buffer", "128")
    .set("spark.driver.allowMultipleContexts", "true")
    .set("spark.sql.tungsten.enabled", "true").setMaster("local[*]")
  val sc: SparkContext = new SparkContext(conf)

  val gse = new GMQLSparkExecutor(
    testingIOFormats = false,
    sc = sc,
    outputFormat = GMQLSchemaFormat.VCF,
    outputCoordinateSystem = GMQLSchemaCoordinateSystem.Default)


  /** Starts the execution */
  override def go(): Unit = {
    println("####  Federated #####")
    implementation()
  }

  override def collect(iRVariable: IRVariable): Any = println("Collect")

  override def take(iRVariable: IRVariable, n: Int): Any = println("take")

  /** stop GMQL implementation (kill a job) */
  override def stop(): Unit = println("stop")

  /** given the name of a parser, returns it. It must at least provide a parser for the "default" name */
  override def getParser(name: String, dataset: String): GMQLLoaderBase = {
    println("getParser")
    gse.getParser(name, dataset)
  }


  def implementation(): Unit = {


    def getLocation(iROperator: IROperator): OperatorAnnotation = {
      iROperator
        .annotations
        .find(_.isInstanceOf[EXECUTED_ON])
        .map(_.asInstanceOf[EXECUTED_ON])
        .getOrElse(EXECUTED_ON(LOCAL_INSTANCE))
    }


    //    def newRecursive(curr: IROperator, deps: List[IROperator]): List[IROperator] = {
    //      if(deps.nonEmpty) {
    //        if (deps.head.isMetaJoinOperator && (getLocation(deps.head) != getLocation(curr))) {
    //          val newCurr = curr.substituteDependency(deps.head, IRReadFedMD())
    //          val other = IRStoreFedMetaJoin(deps.head.asInstanceOf[MetaJoinOperator])
    //          newRecursive(other, other.getDependencies) ::: newRecursive(newCurr, deps.tail)
    //        } else {
    //          val restOfDAG = curr.substituteDependency()
    //        }
    //      }
    //      else List(curr)
    //    }
    //
    //    newRecursive(dag, dag.getDp)


    val splitIdCounter= new AtomicInteger()
    def recursive(currIn: IROperator): List[IROperator] = {

      var curr = currIn
      val currLocation: OperatorAnnotation = getLocation(curr)

      val dep = curr.getDependencies
      //      println("curr:" + curr)
      //      println("dep:" + dep)


      val toDetach = dep.filter(getLocation(_) != currLocation)
      val notToDetach = dep.filter(getLocation(_) == currLocation)


      val newDags: List[IROperator] = toDetach.map { detDag =>
        val splitId = splitIdCounter.getAndIncrement()

        val readIR: IROperator =
          if (detDag.isMetaOperator) {
            IRReadFedMD()
          }
          else if (detDag.isRegionOperator) {
            IRReadFedRD()
          }
          else if (detDag.isMetaGroupOperator) {
            IRReadFedMetaGroup()
          }
          else if (detDag.isMetaJoinOperator) {
            IRReadFedMetaJoin()
          }
          else
            throw new Exception("Unknown type")

        readIR.addAnnotation(getLocation(curr))
        readIR.addAnnotation(SPLIT_ID(splitId))
        curr.substituteDependency(detDag, readIR)

        val storeIR: IROperator =
          if (detDag.isMetaOperator)
            IRStoreFedMD(detDag.asInstanceOf[MetaOperator])
          else if (detDag.isRegionOperator)
            IRStoreFedRD(detDag.asInstanceOf[RegionOperator])
          else if (detDag.isMetaJoinOperator)
            IRStoreFedMetaJoin(detDag.asInstanceOf[MetaJoinOperator])
          else if (detDag.isMetaGroupOperator)
            IRStoreFedMetaGroup(detDag.asInstanceOf[MetaGroupOperator])
          else
            throw new Exception("Unknown type")


        storeIR.addAnnotation(getLocation(detDag))
        storeIR.addAnnotation(SPLIT_ID(splitId))
        storeIR
      }
      newDags ++ notToDetach.flatMap(recursive) ++ newDags.flatMap(recursive)
    }

    def getSubDags(metadag: IROperator, regiondag: IROperator): List[(IROperator, IROperator)] = {

      val metaAddress = getLocation(metadag)
      val regAddress = getLocation(regiondag)

      var dag = List.empty[IROperator]
      if (metaAddress == regAddress) {
        println("metadag:" + metadag)

        println("metadag-dep" + metadag.getDependencies)

        println("------")


        println("---R---")
        try {
          val recRes = List(metadag) ++ recursive(metadag)

          recRes.foreach { t =>
            println("--DAG--")
            println(t)


            println("--END_DAG--")

          }
          val opDAG = new OperatorDAG(recRes)

          val operatorDAGFrame = new OperatorDAGFrame(opDAG, squeeze = false)
          operatorDAGFrame.setSize(1000, 600)
          operatorDAGFrame.setVisible(true)
        }
        catch {
          case e: Exception => e.printStackTrace()
        }

        println("---R--")


      }

      List.empty
    }

    val stack = Stack[FederatedStep]()
    for (variable <- to_be_materialized) {

      val metaAddress = getLocation(variable.metaDag)
      val regAddress = getLocation(variable.regionDag)


      getSubDags(variable.metaDag, variable.regionDag)

      if (metaAddress != regAddress) {
        println("ERRORE")
        sys.exit(0)
      }
      //          println(variable.metaDag)
      //          println(variable.regionDag)
    }
  }
}


