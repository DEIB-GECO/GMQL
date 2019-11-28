package it.polimi.genomics.core.Debug

import it.polimi.genomics.core.DAG.{DAGDraw, OperatorDAGFrame, VariableDAG}
import it.polimi.genomics.core.DataStructures.ExecutionParameters.BinningParameter
import it.polimi.genomics.core.DataStructures._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

object DAGInjector {

  import java.util.UUID.randomUUID


  /* DAG is a list of IRVariables, each representing a variable to be materialized.
   * within each variable there is reference to the actual DAG of IROperators that generates that variable.
   * Specifically, there are: regionDag and metaDag, associated, respectively, to the materialization operators
   * of regions (RegionOperator < IROperator) and meta (Metaoperator < IROperator)
   */


  private final val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private final val binningPar = BinningParameter(Some(1000)) //todo: check

  private def log(text: String) = {
    println(text)
    //logger.debug(text)
  }

  private def editDAG(node: IROperator): IROperator = {

    // Get the dependencies of the current node
    val dependencies: Seq[IROperator] = node.getDependencies

    var editedDAG = node

    for (d <- dependencies) {

      // Since with substituteDependency annotations are lost, I save them before and put them back later
      val old_annotations = node.annotations

      d match {
        case r: RegionOperator     => editedDAG = editedDAG.substituteDependency(r, IRDebugRD(editDAG(r).asInstanceOf[RegionOperator]))
        case m: MetaOperator       => editedDAG = editedDAG.substituteDependency(m, IRDebugMD(editDAG(m).asInstanceOf[MetaOperator]))
        case mg: MetaGroupOperator => editedDAG = editedDAG.substituteDependency(mg, IRDebugMG(editDAG(mg).asInstanceOf[MetaGroupOperator]))
        case mj: MetaJoinOperator  => editedDAG = editedDAG.substituteDependency(mj, IRDebugMJ(editDAG(mj).asInstanceOf[MetaJoinOperator]))
        case _ => throw new Exception("Not recognized operator type.")
      }

      editedDAG.annotations ++= old_annotations

    }

    editedDAG
  }

  private def logOrigin(root: IROperator): Unit = {
    root.getDependencies.foreach(logOrigin)
    log(root.getClass.getName+" ("+root.getOperator+")")
  }

  def inject(DAG:mutable.MutableList[IRVariable], debug:Boolean=true, show:Boolean=false) : mutable.MutableList[IRVariable] = {

    // Log the original DAG
    if(debug)
      DAG.foreach(v=>{logOrigin(v.metaDag); logOrigin(v.regionDag)})

    // Show the original DAG
    if(show) {
      val vd = new VariableDAG(DAG.toList)
      val operatorDAGFrame = new OperatorDAGFrame(vd.toOperatorDAG, squeeze = true)
      DAGDraw.showFrame(operatorDAGFrame, "Original DAG")
    }


    // DAG containing the injected debug operators
    var editedDAG: mutable.MutableList[IRVariable] = mutable.MutableList()

    // For each variable in the DAG ( = for each materialize )
    for( v <- DAG) {

      val edit_step1 =  IRVariable(regionDag = editDAG(v.regionDag).asInstanceOf[RegionOperator], metaDag = v.metaDag, schema = v.schema, dependencies = v.dependencies, name=v.name)(binningPar)
      val edit_step2 =  IRVariable(regionDag = edit_step1.regionDag, metaDag = editDAG(v.metaDag).asInstanceOf[MetaOperator], schema = v.schema, dependencies = v.dependencies, name=v.name)(binningPar)

      editedDAG = editedDAG :+ edit_step2

    }


    def getDebugOp(o:IROperator) : IROperator = {
       o match {
        case r: RegionOperator     => IRDebugRD(r.asInstanceOf[RegionOperator])
        case m: MetaOperator       => IRDebugMD(m.asInstanceOf[MetaOperator])
        case mg: MetaGroupOperator => IRDebugMG(mg.asInstanceOf[MetaGroupOperator])
        case mj: MetaJoinOperator  => IRDebugMJ(mj.asInstanceOf[MetaJoinOperator])
        case _ => throw new Exception("Not recognized operator type.")
      }
    }

    editedDAG = editedDAG.map(v =>
      IRVariable( regionDag = getDebugOp(v.regionDag).asInstanceOf[RegionOperator], metaDag = getDebugOp(v.metaDag).asInstanceOf[MetaOperator], schema = v.schema, dependencies = v.dependencies, name = v.name)(binningPar)
    )




    // Show the new DAG
    val vd = new VariableDAG(editedDAG.toList)
    val operatorDAGFrame = new OperatorDAGFrame(vd.toOperatorDAG, squeeze = true)
    DAGDraw.showFrame(operatorDAGFrame, "New DAG")

    editedDAG

  }

}
