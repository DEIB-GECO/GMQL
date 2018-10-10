package it.polimi.genomics.core.DAG

import com.mxgraph.layout.hierarchical.mxHierarchicalLayout
import com.mxgraph.swing.mxGraphComponent
import com.mxgraph.view.mxGraph
import it.polimi.genomics.core.DataStructures._
import javax.swing.JFrame

object DAGDraw {
  def getVertexDims(text: String): (Double, Double) = {
    val lines = text.split("\n")
    val nLines = lines.length
    val maxWidth = lines.map(x => x.length).max

    (maxWidth * 10, nLines * 20)
  }
}


abstract class DAGFrame[T <: DAGNode[T]](dag: GenericDAG[T, _], squeeze: Boolean = true) extends JFrame {
  protected final val ORANGE = "#f89610"
  protected final val GREEN = "#32e113"

  private val mappingNodeToVertex = collection.mutable.Map[T, Object]()
  private val mappingNodeToOpEdge = collection.mutable.Map[(Object, Object), Object]()

  val graph = new mxGraph
  val graphLayout = new mxHierarchicalLayout(graph)
  val graphParent = graph.getDefaultParent

  graph.getModel.beginUpdate()

  protected def getStyle(node: T): String

  protected def getText(node: T): String

  private def drawDAG(node: T): Object = {
    val style = getStyle(node)
    val nodeSize = DAGDraw.getVertexDims(getText(node))

    val nodeVertex = {
      if (mappingNodeToVertex.contains(node) && this.squeeze)
        mappingNodeToVertex(node)
      else {
        val v = graph.insertVertex(graphParent, null, getText(node), 0, 0, nodeSize._1, nodeSize._2, style)
        mappingNodeToVertex += node -> v
        v
      }
    }
    if (node.getDependencies.nonEmpty) {
      val childVertices = node.getDependencies.map(drawDAG)
      childVertices.map(x => {
        if (mappingNodeToOpEdge.contains((x, nodeVertex)) && this.squeeze)
          mappingNodeToOpEdge((x, nodeVertex))
        else {
          val e = graph.insertEdge(graphParent, null, "", x, nodeVertex)
          mappingNodeToOpEdge += (x, nodeVertex) -> e
          e
        }
      })
    }
    nodeVertex
  }

  this.dag.roots.map(drawDAG)
  graph.setAllowDanglingEdges(false)
  graph.setConnectableEdges(false)
  graph.setHtmlLabels(true)

  graphLayout.execute(graphParent)
  graph.getModel().endUpdate()

  val graphComponent = new mxGraphComponent(graph)
  this.getContentPane.add(graphComponent)
}


class OperatorDAGFrame(dag: OperatorDAG, squeeze: Boolean = true) extends DAGFrame[IROperator](dag, squeeze) {
  override protected def getStyle(node: IROperator): String = {
    val fillColor = "fillColor=" + {
      if (node.isMetaOperator) ORANGE
      else GREEN
    } + ";"
    fillColor
  }

  override protected def getText(node: IROperator): String = node.toString
}


class VariableDAGFrame(dag: VariableDAG, squeeze: Boolean = true) extends DAGFrame[IRVariable](dag, squeeze) {
  override protected def getStyle(node: IRVariable): String = ""

  override protected def getText(node: IRVariable): String = "<b>" + node.toString + "</b>" +
    "<i>" + (if (node.annotations.nonEmpty) "\n" + node.annotations.mkString(",") else "") + "</i>"
}

class MetaDAGFrame(dag: MetaDAG, squeeze: Boolean = true) extends DAGFrame[ExecutionDAG](dag, squeeze) {
  override protected def getStyle(node: ExecutionDAG): String = "fillColor=" + GREEN

  override protected def getText(node: ExecutionDAG): String = {

    def getREADstring(node: ExecutionDAG): String = {

      def getREADs(op: IROperator): List[IROperator] = {
        op match {
          case IRReadMD(_, _, _) => List(op)
          case IRReadRD(_, _, _) => List(op)
          case IRReadFedRD(_, _) => List(op)
          case IRReadFedMD(_, _) => List(op)
          case IRReadFedMetaJoin(_, _) => List(op)
          case IRReadFedMetaGroup(_, _) => List(op)
          case _ => op.getDependencies.flatMap(getREADs)
        }
      }

      node.dag.flatMap(x => x.roots).flatMap(getREADs).distinct.mkString("\n+\n")
    }

    val stores = node.dag.map(x => x.roots.mkString(",")).mkString("\n+\n")
    val reads = getREADstring(node)

    reads + "\n|\n|\nV\n" + stores
  }
}