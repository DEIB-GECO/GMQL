package it.polimi.genomics.core.DAG

import com.mxgraph.view.mxStylesheet
import com.mxgraph.util.mxConstants
import com.mxgraph.view.mxGraph
import com.mxgraph.swing.mxGraphComponent
import javax.swing.{JFrame, WindowConstants}
import java.awt.Dimension
import java.awt.Toolkit

import scala.collection.JavaConversions._

import it.polimi.genomics.core.DataStructures.IROperator

class DAGView(val dag: DAG,  val name:String) extends JFrame {

  // Maps the IROperator to the respective graph component
  val mapping: collection.mutable.Map[IROperator, AnyRef] = collection.mutable.Map[IROperator, AnyRef]()

  setTitle(name)

  // Colors definition
  val BLUE  = "#0471ce"
  val BLUE1 = "#658df8"
  val ORANGE = "#f89610"
  val ORANGE1 = "#eb7b27"
  val WHITE = "#FFFFFF"

  // Create a centered window
  setPreferredSize(new Dimension(700, 500))
  pack() // avoid repaint bug when using LaF-decoration
  val dim: Dimension = Toolkit.getDefaultToolkit.getScreenSize
  this.setLocation(dim.width / 2 - this.getSize.width / 2, dim.height / 2 - this.getSize.height / 2)
  setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE)

  // Initialize the graph
  val graph = new mxGraph

  // Set general style for the graph
  val stylesheet = new mxStylesheet
  stylesheet.getDefaultVertexStyle.put(mxConstants.STYLE_FONTCOLOR, "black")
  graph.setStylesheet(stylesheet)

  val parentSome: Any = graph.getDefaultParent

  // Start graph editing
  graph.getModel.beginUpdate


  // Recursive function used to generate the graph
  def generateDAG(cur: IROperator, cur_vertex: AnyRef, depth: Int, fatherX: Double): Unit = {

    //println("Currently processing "+cur.getClass.getSimpleName+" , mapping.contains "+mapping.keys)

    // check if this vertex was already processed

    var index = 0

    for (child: IROperator <- cur.getDependencies) {
      // println(cur.getClass.getSimpleName+" -> "+child.getClass.getSimpleName)

      var child_vertex: AnyRef = null

      // check if this vertex was already added
      if (mapping.contains(child)) {
        child_vertex = mapping(child)
      } else {
        // compute the x coordinate of the current node and its color
        val newFatherX = fatherX - 50 + 110 * index
        //val style = "fontColor=white ; fillColor=" + (if (child.isMetaOperator) RED else BLUE) +";"
        val style = if (child.isMetaOperator) "fillColor="+ORANGE+";" else ""

        val name = child.getClass.getSimpleName + (if(child.requiresOutputProfile) " *" else "")

        child_vertex = graph.insertVertex(parentSome, null, name , newFatherX, 10 + 100 * depth, 80, 30, style)

        // add the node and the graphic element to the mapping
        mapping += (child -> child_vertex)

        // go on with the recursion processing the children
        generateDAG(child, child_vertex, depth + 1, newFatherX)
      }

      graph.insertEdge(parentSome, null, "", cur_vertex, child_vertex)

      index += 1
    }
  }


  var index = 0

  for (node <- dag.raw) {

    val style = "fillColor=" + (if (node.isMetaOperator) ORANGE1 else BLUE1)
    val fatherX = 80*dag.depth + index * 40 * dag.maxWidth
    val v = graph.insertVertex(parentSome, null, node.getClass.getSimpleName, fatherX, 10, 80, 30, style)


    generateDAG(node, v, 1, fatherX)
    index += 1
  }

  // Stop graph editing
  graph.getModel().endUpdate()

  // parentSome.setEnabled(false)

  // Make the graph visible
  val graphComponent = new mxGraphComponent(graph)
  getContentPane.add(graphComponent)

  setVisible(true)
}

