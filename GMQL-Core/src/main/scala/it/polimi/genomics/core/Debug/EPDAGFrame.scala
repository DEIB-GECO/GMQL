package it.polimi.genomics.core.Debug

import com.mxgraph.layout.hierarchical.mxHierarchicalLayout
import com.mxgraph.swing.mxGraphComponent
import com.mxgraph.util.mxConstants
import com.mxgraph.view.mxGraph
import javax.swing.JFrame



object EPDAGDraw {
  def getVertexDims(text: String): (Double, Double) = {
    val lines = text.split("\n")
    val nLines = lines.length
    val maxWidth = lines.map(x => x.length).max

    (maxWidth * 10, nLines * 20)

  }

  def showFrame(dagFrame: EPDAGFrame, title: String): Unit = {
    dagFrame.setSize(1000, 600)
    dagFrame.setVisible(true)
    dagFrame.setTitle(title)
  }
}

class EPDAGFrame(dag: EPDAG) extends JFrame {
  protected final val ORANGE = "#f89610"
  protected final val GREEN = "#32e113"
  protected final val GREY = "lightblue"

  private val mappingNodeToVertex = collection.mutable.Map[EPNode, Object]()
  private val mappingNodeToOpEdge = collection.mutable.Map[(Object, Object), Object]()

  val graph = new mxGraph
  val graphLayout = new mxHierarchicalLayout(graph)
  val graphParent = graph.getDefaultParent

  graph.getModel.beginUpdate()

  private def getStyle = mxConstants.STYLE_FILLCOLOR+"="+GREEN+";"+mxConstants.STYLE_ALIGN+"="+mxConstants.ALIGN_LEFT

  private def getText(node: EPNode) = node.toString

  private def drawDAG(node: EPNode): Object = {
    val style = getStyle
    val nodeSize = EPDAGDraw.getVertexDims(getText(node))

    val nodeVertex = {
      if (mappingNodeToVertex.contains(node))
        mappingNodeToVertex(node)
      else {
        val v = graph.insertVertex(graphParent, null, getText(node), 0, 0, nodeSize._1, nodeSize._2, style)
        mappingNodeToVertex += node -> v
        v
      }
    }
    if (node.getParents.nonEmpty) {
      val childVertices = node.getParents.map(drawDAG)
      childVertices.map(x => {
        if (mappingNodeToOpEdge.contains((x, nodeVertex)))
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

  this.dag.exitNodes.map(drawDAG)
  graph.setAllowDanglingEdges(false)
  graph.setConnectableEdges(false)
  graph.setHtmlLabels(true)

  graphLayout.execute(graphParent)
  graph.getModel.endUpdate()

  val graphComponent = new mxGraphComponent(graph)
  this.getContentPane.add(graphComponent)
}
