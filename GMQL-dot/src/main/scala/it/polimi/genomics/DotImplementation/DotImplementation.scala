package it.polimi.genomics.DotImplementation

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import it.polimi.genomics.GMQLServer.Implementation
import it.polimi.genomics.core.DataStructures.GroupMDParameters.{TopG, Top, NoTop}
import it.polimi.genomics.core.DataStructures.GroupRDParameters.FIELD
import it.polimi.genomics.core.DataStructures.RegionAggregate.RegionsToRegion
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.{DataTypes, GMQLLoader, ParsingType, GMQLLoaderBase}
import org.slf4j.LoggerFactory

import scala.collection.mutable.HashSet

class FakeParser extends GMQLLoader[(Long,String), DataTypes.FlinkRegionType, (Long,String), DataTypes.FlinkMetaType]  {
  override def meta_parser(t : (Long, String)) = null
  override def region_parser(t : (Long, String)) = null
}

object BedScoreParser extends FakeParser {
  schema = List(("score", ParsingType.DOUBLE))
}
object RnaSeqParser extends FakeParser {
  schema = List(("name", ParsingType.STRING), ("score", ParsingType.DOUBLE))
}

class DotImplementation(dot_graph_path : String = "./file.dot",
                        generated_img_path : String = "./file.png",
                        img_type : String = "png",
                        shorten_paths : Boolean = false) extends Implementation {

  final val logger = LoggerFactory.getLogger(this.getClass)
  def getParser(name : String,dataset:String) : GMQLLoaderBase = {
    name match {
      case "BedScoreParser" => BedScoreParser
      case "RnaSeqParser" => RnaSeqParser
      case "default" => BedScoreParser
      case _ => {logger.warn("unable to find " + name + " parser, try the default one"); getParser("default",dataset)}
    }
  }

  override def stop(): Unit = ???

  def go(): Unit = {
    val dot_code = toDotGraph(to_be_materialized(0))
    Files.write(Paths.get(dot_graph_path), dot_code.getBytes(StandardCharsets.UTF_8))
    val cmd = img_type match {
      case "png" => "dot " + dot_graph_path + " -Tpng -o " + generated_img_path
      case _ => throw new Exception("Unknown image type " + img_type)
    }
    val dp = Runtime.getRuntime.exec(cmd)
    dp.waitFor()
  }

  def toDotGraph(variable : IRVariable)  : String = {
try{
    val all_nodes = (add_all_nodes(variable.metaDag.asInstanceOf[IROperator]) ++ add_all_nodes(variable.regionDag.asInstanceOf[IROperator])).toList
    val sb: StringBuilder = new StringBuilder

    def edge(a: Int, b: Int) = "struct" + a + "-> struct" + b + ";\n"


    sb append "digraph GMQLgraph {\n"
    sb append "rankdir=BT; \n"

    for (i <- 0 to all_nodes.length - 1) sb append nodeToString(i, all_nodes(i))

    for (i <- 0 to all_nodes.length - 1) {
      all_nodes(i) match {
        case IRPurgeMD(l, r) => sb append edge(i, all_nodes.indexOf(l)) append edge(i, all_nodes.indexOf(r))
        case IRPurgeRD(l, r) => sb append edge(i, all_nodes.indexOf(l)) append edge(i, all_nodes.indexOf(r))
        case IRSelectRD(_, None, p) => sb append edge(i, all_nodes.indexOf(p))
        case IRSelectRD(_, Some(c), p) => sb append edge(i, all_nodes.indexOf(p)) append edge(i, all_nodes.indexOf(c))
        case IRSelectMD(_, p) => sb append edge(i, all_nodes.indexOf(p))
        case IRStoreMD(_, p, _) => sb append edge(i, all_nodes.indexOf(p))
        case IRStoreRD(_, p, _) => sb append edge(i, all_nodes.indexOf(p))
        case IRSemiJoin(l, _, r) => sb append edge(i, all_nodes.indexOf(l)) append edge(i, all_nodes.indexOf(r))
        case IRProjectMD(_, _, p) => sb append edge(i, all_nodes.indexOf(p))
        case IRUnionMD(l, r, _, _) => sb append edge(i, all_nodes.indexOf(l)) append edge(i, all_nodes.indexOf(r))
        case IRAggregateRD(_, p) => sb append edge(i, all_nodes.indexOf(p))
        case IRProjectRD(_, _, p) => sb append edge(i, all_nodes.indexOf(p))
        case IRGroupMD(_, _, _, p, r) => sb append edge(i, all_nodes.indexOf(p)) append edge(i, all_nodes.indexOf(r))
        case IRGroupRD(_, _, p) => sb append edge(i, all_nodes.indexOf(p))
        case IROrderMD(_, _, _, p) => sb append edge(i, all_nodes.indexOf(p))
        case IROrderRD(_, _, p) => sb append edge(i, all_nodes.indexOf(p))
        case IRGroupBy(_, p) => sb append edge(i, all_nodes.indexOf(p))
        case IRRegionCover(_, _, _, _, None, p) => sb append edge(i, all_nodes.indexOf(p))
        case IRRegionCover(_, _, _, _, Some(c), p) => sb append edge(i, all_nodes.indexOf(p)) append edge(i, all_nodes.indexOf(c))
        case IRJoinBy(_, l, r) => sb append edge(i, all_nodes.indexOf(l)) append edge(i, all_nodes.indexOf(r))
        //        case IRCombineMD(None,l,r,_,_) => sb append edge(i, all_nodes.indexOf(l)) append edge(i, all_nodes.indexOf(r))
        case IRCombineMD(c, l, r, _, _) => if (c.isInstanceOf[SomeMetaJoinOperator]) {
          sb append edge(i, all_nodes.indexOf(c.asInstanceOf[SomeMetaJoinOperator].operator)) append edge(i, all_nodes.indexOf(l)) append edge(i, all_nodes.indexOf(r))
        } else {
          sb append edge(i, all_nodes.indexOf(c.asInstanceOf[NoMetaJoinOperator].operator)) append edge(i, all_nodes.indexOf(l)) append edge(i, all_nodes.indexOf(r))
        }
        case IRCollapseMD(None, p) => sb append edge(i, all_nodes.indexOf(p))
        case IRCollapseMD(Some(c), p) => sb append edge(i, all_nodes.indexOf(c)) append edge(i, all_nodes.indexOf(p))
        //        case IRGenometricMap(None,_,l,r) => sb append edge(i, all_nodes.indexOf(l)) append edge(i, all_nodes.indexOf(r))
        case IRGenometricMap(c, _, l, r) => if (c.isInstanceOf[SomeMetaJoinOperator]) {
          sb append edge(i, all_nodes.indexOf(c.asInstanceOf[SomeMetaJoinOperator].operator)) append edge(i, all_nodes.indexOf(l)) append edge(i, all_nodes.indexOf(r))
        } else {
          sb append edge(i, all_nodes.indexOf(c.asInstanceOf[NoMetaJoinOperator].operator)) append edge(i, all_nodes.indexOf(l)) append edge(i, all_nodes.indexOf(r))
        }
        case IRUnionRD(_, l, r) => sb append edge(i, all_nodes.indexOf(l)) append edge(i, all_nodes.indexOf(r))
        case IRMergeMD(p, None) => sb append edge(i, all_nodes.indexOf(p))
        case IRMergeMD(p, Some(g)) => sb append edge(i, all_nodes.indexOf(p)) append edge(i, all_nodes.indexOf(g))
        case IRMergeRD(p, None) => sb append edge(i, all_nodes.indexOf(p))
        case IRMergeRD(p, Some(g)) => sb append edge(i, all_nodes.indexOf(p)) append edge(i, all_nodes.indexOf(g))
        //        case IRDifferenceRD(None,l,r) => sb append edge(i, all_nodes.indexOf(l)) append edge(i, all_nodes.indexOf(r))
        case IRDifferenceRD(c, l, r, _) => if (c.isInstanceOf[SomeMetaJoinOperator]) {
          sb append edge(i, all_nodes.indexOf(c.asInstanceOf[SomeMetaJoinOperator].operator)) append edge(i, all_nodes.indexOf(l)) append edge(i, all_nodes.indexOf(r))
        } else {
          sb append edge(i, all_nodes.indexOf(c.asInstanceOf[NoMetaJoinOperator].operator)) append edge(i, all_nodes.indexOf(l)) append edge(i, all_nodes.indexOf(r))
        }
        case IRGenometricJoin(c, _, _, l, r) => if (c.isInstanceOf[SomeMetaJoinOperator]) {
          sb append edge(i, all_nodes.indexOf(c.asInstanceOf[SomeMetaJoinOperator].operator)) append edge(i, all_nodes.indexOf(l)) append edge(i, all_nodes.indexOf(r))
        } else {
          sb append edge(i, all_nodes.indexOf(c.asInstanceOf[NoMetaJoinOperator].operator)) append edge(i, all_nodes.indexOf(l)) append edge(i, all_nodes.indexOf(r))
        }
        //        case IRGenometricJoin(None,_,_,l,r) => sb append edge(i, all_nodes.indexOf(l)) append edge(i, all_nodes.indexOf(r))
        case _ =>
      }
    }

    sb append "{rank=same; "
    for (i <- 0 to all_nodes.length - 1) {

      all_nodes(i) match {
        case IRReadMD(_, _, _) => sb append ("struct" + i + " ")
        case IRReadRD(_, _, _) => sb append ("struct" + i + " ")
        case _ =>
      }

    }
    sb append "}\n"

    sb append "{rank=same; "
    for (i <- 0 to all_nodes.length - 1) {

      all_nodes(i) match {
        case IRStoreMD(_, _, _) => sb append ("struct" + i + " ")
        case IRStoreRD(_, _, _) => sb append ("struct" + i + " ")
        case _ =>
      }

    }
    sb append "}\n"
    sb append "}\n"
    sb.toString()

  }catch{ case ex:Throwable =>ex.printStackTrace();""}
  }

  def nodeToString(pos : Int, n : IROperator ) : String = {
    n match  {
      case IRReadMD(path, loader,_) => "struct" + pos +
        " [shape=record,color=\"blue\",label=\"{IRReadMD | path=" +
        {if(shorten_paths) path.map(x=>(new File(x)).getName).mkString(",") else {path.mkString(",")}} +
        " | loader=" + loader.getClass.getSimpleName + "}" + "\"];\n"
      case IRReadRD(path, loader, _) =>  "struct" + pos + " [shape=record,color=\"red\",label=\"{IRReadRD | path=" +
        {if(shorten_paths) path.map(x=>(new File(x)).getName).mkString(",") else {path.mkString(",")}} +
        " | loader=" + loader.getClass.getSimpleName + "}" + "\"];\n"
      case IRPurgeMD(_, _) => "struct" + pos + " [shape=record,color=\"blue\",label=\"IRPurgeMD" + "\"];\n"
      case IRPurgeRD(_, _) => "struct" + pos + " [shape=record,color=\"red\",label=\"IRPurgeRD" + "\"];\n"
      case IRSelectRD(p, _, _) => "struct" + pos + " [shape=record,color=\"red\",label=\"{IRSelectRD | " + p + "}" + "\"];\n"
      case IRSelectMD(p, _) => "struct" + pos + " [shape=record,color=\"blue\",label=\"{IRSelectMD | " + p + "}" + "\"];\n"
      case IRStoreMD(_,_,_) => "struct" + pos + " [label=\"StoreMD\",color=\"blue\"];\n"
      case IRStoreRD(_,_,_) => "struct" + pos + " [label=\"StoreRD\",color=\"red\"];\n"
      case IRSemiJoin(_,c,_) =>"struct" +  pos +  " [shape=record,color=\"blue\",label=\"{IRSemiJoin | " + c +  "}" + "\"];\n"
      case IRProjectMD(p,e,_) => "struct" + pos + " [shape=record,color=\"blue\",label=\"{IRProjectMD | " + "projected attributes=" + {if(p.isDefined)  (p.get mkString ",") else p} + " | meta aggregate=" +  {if(e.isDefined) (e.get.getClass.getSimpleName) else e} +  "}\"];\n"
      case IRUnionMD(_,_,_,_) => "struct" + pos + " [shape=record,color=\"blue\",label=\"IRUnionMD" + "\"];\n"
      case IRUnionAggMD(_,_,_,_) => "struct" + pos + " [shape=record,color=\"blue\",label=\"IRUnionMD" + "\"];\n"
      case IRAggregateRD(p, _) => "struct" + pos + " [shape=record,color=\"red\",label=\"{IRAggregateRD | new attr. name = " + "\"];\n"
      case IRProjectRD(p,e,_) => "struct" + pos + " [shape=record,color=\"red\",label=\"{IRProjectRD | projected fields = " + {if(p.isDefined) (p.get mkString ",") else p} + " | tuple function = " + {if(e.isDefined) e.get.getClass.getSimpleName else e} +  "}\"];\n"
      //TODO di nuovo modificata
      //case IRGroupMD(k,a,g,_) => "struct" + pos + " [shape=record,color=\"blue\",label=\"{IRGroupMD | keys=" + (k mkString ",") + "|" + a + " | new_attribute=" + g +  "\"];\n"
      case IRGroupMD(k,a,g,_,_) => "struct" + pos + " [shape=record,color=\"blue\",label=\"{IRGroupMD | keys=" + (k.attributes mkString ",") + "|" + a + " | new_attribute=" + g +  "\"];\n"
      case IROrderMD(o,n,t,_) => "struct" + pos + " [shape=record,color=\"blue\",label=\"{IROrderMD | ord=" + (o mkString ",") + "|" + "new_attribute=" + n + "|top="+ t + "\"];\n"
      case IRGroupBy(m, _) => "struct" + pos + " [shape=record,color=\"green\",label=\"{IRGroupBy | by : " + (m.attributes mkString ",") + "}\"];\n"
      case IRRegionCover(t,n,x,a,_,_) => "struct" + pos + " [shape=record,color=\"red\",label=\"{IRregionCover | type=" + t + "|min=" + n + "|max=" +x+ "|aggregates=" + (a mkString ",") + "}\"];\n"
      case IRJoinBy(c,_,_) => "struct" + pos + " [shape=record,color=\"green\",label=\"{IRJoinBy | keys=" + (c.attributes mkString ",") + "}\"];\n"
      case IRCombineMD(_,_,_,_,_) => "struct" + pos + " [shape=record,color=\"blue\",label=\"IRCombineMD" + "\"];\n"
      case IRCollapseMD(_,_) => "struct" + pos + " [shape=record,color=\"blue\",label=\"IRCollapseMD" + "\"];\n"
      case IRGenometricMap(_,a,_,_) => "struct" + pos + " [shape=record,color=\"red\",label=\"{IRGenometricMap |" + (a map {_.getClass.getSimpleName} mkString " | ") + "}\"];\n"
      case IRUnionRD(l,_,_) => "struct" + pos + " [shape=record,color=\"red\",label=\"{IRMergeRD | Schema: " + (l mkString ",") + "}\"];\n"
      case IRGroupRD(p,a,_) => "struct" + pos + " [shape=record,color=\"blue\",label=\"{IRGroupRD " + {if(p.isDefined) " | keys= " + {p.get.map(x=>x.asInstanceOf[FIELD].position).mkString("\t")} else ""} + {if(p.isDefined) " | aggregates= " + {a.get.map(x=>x.asInstanceOf[RegionsToRegion].getClass.getSimpleName).mkString("\t")} else ""} +  "}\"];\n"
      case IROrderRD(o,t,_) => "struct" + pos + " [shape=record,color=\"red\",label=\"{IROrderRD | ord=" + o.mkString(",") +  {t match {case NoTop() => "";case Top(k) => " | Top " + k ; case TopG(k) => " | TopG " + k;} }+  "}\"];\n"
      case IRMergeMD(_,_) => "struct" + pos + " [shape=record,color=\"blue\",label=\"{IRMergeMD " +  "}\"];\n"
      case IRMergeRD(_,_) => "struct" + pos + " [shape=record,color=\"red\",label=\"{IRMergeRD " +  "}\"];\n"
      case IRDifferenceRD(_,_,_,_) => "struct" + pos + " [shape=record,color=\"blue\",label=\"{IRDifferenceRD " +  "}\"];\n"
      case IRGenometricJoin(_,c,b,_,_) =>  "struct" + pos + " [shape=record,color=\"red\",label=\"{IRGenometricJoin | cond=" + c.mkString(",") + " | builder= " + b + "}\"];\n"
    }
  }

  def add_all_nodes(node : IROperator) : HashSet[IROperator] = {
    node match {
      case IRReadMD(_,_,_) => (new HashSet()) + node
      case IRReadRD(_,_,_) => (new HashSet()) + node
      case IRPurgeMD(l,r) => (add_all_nodes(l.asInstanceOf[IROperator]) ++ add_all_nodes(r.asInstanceOf[IROperator])) + node
      case IRPurgeRD(l,r) => (add_all_nodes(l.asInstanceOf[IROperator]) ++ add_all_nodes(r.asInstanceOf[IROperator])) + node
      case IRSelectMD(_,p) => add_all_nodes(p.asInstanceOf[IROperator]) + node
      case IRSelectRD(_,None,p) => add_all_nodes(p.asInstanceOf[IROperator]) + node
      case IRSelectRD(_,Some(c),p) => add_all_nodes(p.asInstanceOf[IROperator]) ++ add_all_nodes(c.asInstanceOf[IROperator]) + node
      case IRStoreMD(_,p,_) => add_all_nodes(p.asInstanceOf[IROperator]) + node
      case IRStoreRD(_,p,_) => add_all_nodes(p.asInstanceOf[IROperator]) + node
      case IRSemiJoin(l,_,r) => (add_all_nodes(l.asInstanceOf[IROperator]) ++ add_all_nodes(r.asInstanceOf[IROperator])) + node
      case IRProjectMD(_,_,p) => add_all_nodes(p.asInstanceOf[IROperator]) + node
      case IRUnionMD(l,r,_,_) => (add_all_nodes(l.asInstanceOf[IROperator]) ++ add_all_nodes(r.asInstanceOf[IROperator])) + node
      case IRUnionAggMD(l,r,_,_) => (add_all_nodes(l.asInstanceOf[IROperator]) ++ add_all_nodes(r.asInstanceOf[IROperator])) + node
      case IRAggregateRD(_,p) => add_all_nodes(p.asInstanceOf[IROperator]) + node
      case IRProjectRD(_,_,p) => add_all_nodes(p.asInstanceOf[IROperator]) + node
      case IRGroupMD(_,_,_,p,r) => add_all_nodes(p.asInstanceOf[IROperator]) ++ add_all_nodes(r.asInstanceOf[IROperator]) + node
      case IRGroupRD(_,_,p) => add_all_nodes(p.asInstanceOf[IROperator]) + node
      case IROrderMD(_,_,_,p) => add_all_nodes(p.asInstanceOf[IROperator]) + node
      case IRGroupBy(_,p) => add_all_nodes(p.asInstanceOf[IROperator]) + node
      case IRRegionCover(_,_,_,_,None,p) => add_all_nodes(p.asInstanceOf[IROperator]) + node
      case IRRegionCover(_,_,_,_,Some(c),p) => add_all_nodes(p.asInstanceOf[IROperator]) ++ add_all_nodes(c.asInstanceOf[IROperator]) + node
      case IRJoinBy(_,l,r) => (add_all_nodes(l.asInstanceOf[IROperator]) ++ add_all_nodes(r.asInstanceOf[IROperator])) + node
//      case IRCombineMD(None,l,r,_,_) =>  (add_all_nodes(l.asInstanceOf[IROperator]) ++ add_all_nodes(r.asInstanceOf[IROperator])) + node
      case IRCombineMD(c,l,r,_,_) => if(c.isInstanceOf[SomeMetaJoinOperator]) {(add_all_nodes(c.asInstanceOf[SomeMetaJoinOperator].operator.asInstanceOf[IROperator]) ++ add_all_nodes(l.asInstanceOf[IROperator]) ++ add_all_nodes(r.asInstanceOf[IROperator])) + node} else {(add_all_nodes(c.asInstanceOf[NoMetaJoinOperator].operator.asInstanceOf[IROperator]) ++ add_all_nodes(l.asInstanceOf[IROperator]) ++ add_all_nodes(r.asInstanceOf[IROperator])) + node}
      case IRCollapseMD(None,p) =>  (add_all_nodes(p.asInstanceOf[IROperator])) + node
      case IRCollapseMD(Some(c),p) =>  (add_all_nodes(c.asInstanceOf[IROperator]) ++ add_all_nodes(p.asInstanceOf[IROperator])) + node
//      case IRGenometricMap(None,_,l,r) => (add_all_nodes(l.asInstanceOf[IROperator]) ++ add_all_nodes(r.asInstanceOf[IROperator])) + node
      case IRGenometricMap(c,_,l,r) => if(c.isInstanceOf[SomeMetaJoinOperator]) {(add_all_nodes(c.asInstanceOf[SomeMetaJoinOperator].operator.asInstanceOf[IROperator]) ++ add_all_nodes(l.asInstanceOf[IROperator]) ++ add_all_nodes(r.asInstanceOf[IROperator])) + node} else {(add_all_nodes(c.asInstanceOf[NoMetaJoinOperator].operator.asInstanceOf[IROperator]) ++ add_all_nodes(l.asInstanceOf[IROperator]) ++ add_all_nodes(r.asInstanceOf[IROperator])) + node}
      case IRUnionRD(_,l,r) => (add_all_nodes(l.asInstanceOf[IROperator]) ++ add_all_nodes(r.asInstanceOf[IROperator])) + node
      case IROrderRD(_,_,p) => add_all_nodes(p.asInstanceOf[IROperator]) + node
      case IRMergeMD(p,None) => add_all_nodes(p.asInstanceOf[IROperator]) + node
      case IRMergeMD(p,Some(g)) => add_all_nodes(p.asInstanceOf[IROperator]) ++ add_all_nodes(g.asInstanceOf[IROperator]) + node
      case IRMergeRD(p,None) => add_all_nodes(p.asInstanceOf[IROperator]) + node
      case IRMergeRD(p,Some(g)) => add_all_nodes(p.asInstanceOf[IROperator]) ++ add_all_nodes(g.asInstanceOf[IROperator]) + node
//      case IRDifferenceRD(None,l,r) => (add_all_nodes(l.asInstanceOf[IROperator]) ++ add_all_nodes(r.asInstanceOf[IROperator])) + node
      case IRDifferenceRD(c,l,r,_) => if(c.isInstanceOf[SomeMetaJoinOperator]) {(add_all_nodes(c.asInstanceOf[SomeMetaJoinOperator].operator.asInstanceOf[IROperator]) ++ add_all_nodes(l.asInstanceOf[IROperator]) ++ add_all_nodes(r.asInstanceOf[IROperator])) + node} else {(add_all_nodes(c.asInstanceOf[NoMetaJoinOperator].operator.asInstanceOf[IROperator]) ++ add_all_nodes(l.asInstanceOf[IROperator]) ++ add_all_nodes(r.asInstanceOf[IROperator])) + node}
//      case IRGenometricJoin(None,_,_,l,r) => (add_all_nodes(l.asInstanceOf[IROperator]) ++ add_all_nodes(r.asInstanceOf[IROperator])) + node
      case IRGenometricJoin(c,_,_,l,r) => if(c.isInstanceOf[SomeMetaJoinOperator]) { (add_all_nodes(c.asInstanceOf[SomeMetaJoinOperator].operator.asInstanceOf[IROperator]) ++ add_all_nodes(l.asInstanceOf[IROperator]) ++ add_all_nodes(r.asInstanceOf[IROperator])) + node} else {(add_all_nodes(c.asInstanceOf[NoMetaJoinOperator].operator.asInstanceOf[IROperator]) ++ add_all_nodes(l.asInstanceOf[IROperator]) ++ add_all_nodes(r.asInstanceOf[IROperator])) + node}
    }
  }

}
