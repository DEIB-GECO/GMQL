package it.polimi.genomics.core.DAG

import com.mxgraph.layout.hierarchical.mxHierarchicalLayout
import com.mxgraph.swing.mxGraphComponent
import com.mxgraph.view.mxGraph
import it.polimi.genomics.core.DataStructures.IRVariable
import javax.swing.JFrame

class VariableDAGFrame(varDag: VariableDAG, squeeze: Boolean = true) extends JFrame {

  private val mappingOpToVertex = collection.mutable.Map[IRVariable, Object]()
  private val mappingOpToOpEdge = collection.mutable.Map[(Object, Object), Object]()

  val graph = new mxGraph
  val graphLayout = new mxHierarchicalLayout(graph)

  val graphParent = graph.getDefaultParent

  graph.getModel.beginUpdate()

  private def drawDAG(node: IRVariable): Object = {
    val nodeSize = DAGDraw.getVertexDims(node.toString)

    val nodeVertex = {
      if(mappingOpToVertex.contains(node) && this.squeeze)
        mappingOpToVertex(node)
      else {
        val v = graph.insertVertex(graphParent, null, node.toString, 0, 0, nodeSize._1, nodeSize._2)
        mappingOpToVertex += node -> v
        v
      }
    }
    if(node.dependencies.nonEmpty){
      val childVertices = node.dependencies.map(drawDAG)
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

  this.varDag.vv.map(drawDAG)
  graph.setAllowDanglingEdges(false)
  graph.setConnectableEdges(false)

  graphLayout.execute(graphParent)
  graph.getModel().endUpdate()

  val graphComponent = new mxGraphComponent(graph)
  this.getContentPane.add(graphComponent)
}
