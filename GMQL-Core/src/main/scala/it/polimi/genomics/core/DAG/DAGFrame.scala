package it.polimi.genomics.core.DAG

import com.mxgraph.layout.hierarchical.mxHierarchicalLayout
import com.mxgraph.swing.mxGraphComponent
import com.mxgraph.view.mxGraph
import it.polimi.genomics.core.DataStructures.IROperator
import javax.swing.JFrame

class DAGFrame(dag: DAG, squeeze: Boolean = true) extends JFrame{

  private final val ORANGE = "#f89610"
  private final val GREEN = "#32e113"

  private val mappingOpToVertex = collection.mutable.Map[IROperator, Object]()
  private val mappingOpToOpEdge = collection.mutable.Map[(Object, Object), Object]()
  val graph = new mxGraph
  val graphLayout = new mxHierarchicalLayout(graph)

  val graphParent = graph.getDefaultParent

  graph.getModel.beginUpdate()

  private def getVertexDims(text: String): (Double, Double) = {
    val lines = text.split("\n")
    val nLines = lines.length
    val maxWidth = lines.map(x => x.length).max

    (maxWidth*10, nLines*20)
  }

  private def getStyle(node: IROperator): String = {
    val fillColor = "fillColor="+ {
      if (node.isMetaOperator) ORANGE
      else GREEN
    } + ";"
    fillColor
  }

  private def drawDAG(node: IROperator): Object = {
    val style = getStyle(node)
    val nodeSize = getVertexDims(node.toString)

    val nodeVertex = {
      if(mappingOpToVertex.contains(node) && this.squeeze)
        mappingOpToVertex(node)
      else {
        val v = graph.insertVertex(graphParent, null, node.toString, 0, 0, nodeSize._1, nodeSize._2, style)
        mappingOpToVertex += node -> v
        v
      }
    }
    if(node.getDependencies.nonEmpty){
      val childVertices = node.getDependencies.map(drawDAG)
      childVertices.map(x => {
        if(mappingOpToOpEdge.contains((x, nodeVertex)) && this.squeeze)
          mappingOpToOpEdge((x, nodeVertex))
        else{
          val e = graph.insertEdge(graphParent, null, "", x, nodeVertex)
          mappingOpToOpEdge += (x, nodeVertex) -> e
          e
        }
      })
    }
    nodeVertex
  }

  this.dag.raw.map(drawDAG)
  graph.setAllowDanglingEdges(false)
  graph.setConnectableEdges(false)

  graphLayout.execute(graphParent)
  graph.getModel().endUpdate()

  val graphComponent = new mxGraphComponent(graph)
  this.getContentPane.add(graphComponent)
}
