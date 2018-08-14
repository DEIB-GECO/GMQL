package it.polimi.genomics.core.DAG

import com.mxgraph.layout.hierarchical.mxHierarchicalLayout
import com.mxgraph.swing.mxGraphComponent
import com.mxgraph.view.mxGraph
import it.polimi.genomics.core.DataStructures.{IROperator, IRVariable}
import javax.swing.JFrame

object DAGDraw {
  def getVertexDims(text: String): (Double, Double) = {
    val lines = text.split("\n")
    val nLines = lines.length
    val maxWidth = lines.map(x => x.length).max

    (maxWidth*10, nLines*20)
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
      if(mappingNodeToVertex.contains(node) && this.squeeze)
        mappingNodeToVertex(node)
      else {
        val v = graph.insertVertex(graphParent, null, getText(node), 0, 0, nodeSize._1, nodeSize._2, style)
        mappingNodeToVertex += node -> v
        v
      }
    }
    if(node.getDependencies.nonEmpty){
      val childVertices = node.getDependencies.map(drawDAG)
      childVertices.map(x => {
        if(mappingNodeToOpEdge.contains((x, nodeVertex)) && this.squeeze)
          mappingNodeToOpEdge((x, nodeVertex))
        else{
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
    val fillColor = "fillColor="+ {
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
    "<i>" + (if(node.annotations.nonEmpty) "\n" + node.annotations.mkString(",") else "") + "</i>"
}


//class DAGFrameOld(dag: OperatorDAG, squeeze: Boolean = true) extends JFrame{
//
//  private final val ORANGE = "#f89610"
//  private final val GREEN = "#32e113"
//
//  private val mappingOpToVertex = collection.mutable.Map[IROperator, Object]()
//  private val mappingOpToOpEdge = collection.mutable.Map[(Object, Object), Object]()
//  val graph = new mxGraph
//  val graphLayout = new mxHierarchicalLayout(graph)
//
//  val graphParent = graph.getDefaultParent
//
//  graph.getModel.beginUpdate()
//
//
//  private def getStyle(node: IROperator): String = {
//    val fillColor = "fillColor="+ {
//      if (node.isMetaOperator) ORANGE
//      else GREEN
//    } + ";"
//    fillColor
//  }
//
//  private def drawDAG(node: IROperator): Object = {
//    val style = getStyle(node)
//    val nodeSize = DAGDraw.getVertexDims(node.toString)
//
//    val nodeVertex = {
//      if(mappingOpToVertex.contains(node) && this.squeeze)
//        mappingOpToVertex(node)
//      else {
//        val v = graph.insertVertex(graphParent, null, node.toString, 0, 0, nodeSize._1, nodeSize._2, style)
//        mappingOpToVertex += node -> v
//        v
//      }
//    }
//    if(node.getDependencies.nonEmpty){
//      val childVertices = node.getDependencies.map(drawDAG)
//      childVertices.map(x => {
//        if(mappingOpToOpEdge.contains((x, nodeVertex)) && this.squeeze)
//          mappingOpToOpEdge((x, nodeVertex))
//        else{
//          val e = graph.insertEdge(graphParent, null, "", x, nodeVertex)
//          mappingOpToOpEdge += (x, nodeVertex) -> e
//          e
//        }
//      })
//    }
//    nodeVertex
//  }
//
//  this.dag.roots.map(drawDAG)
//  graph.setAllowDanglingEdges(false)
//  graph.setConnectableEdges(false)
//
//  graphLayout.execute(graphParent)
//  graph.getModel().endUpdate()
//
//  val graphComponent = new mxGraphComponent(graph)
//  this.getContentPane.add(graphComponent)
//}
