package it.polimi.genomics.GMQLServer

import it.polimi.genomics.core.DataStructures.Builtin.MapFunctionFactory
import it.polimi.genomics.core.DataStructures.RegionAggregate.RegionsToRegion
import it.polimi.genomics.core._
import it.polimi.genomics.core.ParsingType

/**
 * Created by pietro on 22/07/15.
 */
object DefaultRegionsToRegionFactory extends MapFunctionFactory{

  def get(name : String, output_name : Option[String]) = {
    name match {
      case "COUNT" => getCount(output_name)
      case _ => throw new Exception("No map function with the given name (" + name + ") found.")
    }
  }

  def get(name : String, position : Int, output_name : Option[String]) = {
    name match {
      case "SUM" => getSum(position,output_name)
      case "MIN" => getMin(position,output_name)
      case "MAX" => getMax(position,output_name)
      case "AVG" => getAvg(position,output_name)
      case "BAG" => getBAG(position,output_name)
      case _ => throw new Exception("No map function with the given name (" + name + ") found.")
    }
  }

  private def getCount(output_name:Option[String]) = new RegionsToRegion {
    override val resType = ParsingType.DOUBLE
    override val index: Int = 0
    override val associative: Boolean = true
    override val funOut: (GValue,Int) => GValue = {(v1,v2)=>GDouble(v2)}
    override val fun: (List[GValue]) => GValue = {
      (line) =>
        val len = line.size.toDouble
        GDouble(len)
    }}

  private def getSum(position:Int, output_name:Option[String]) = new RegionsToRegion {
    override val resType = ParsingType.DOUBLE
    override val index: Int = position
    override val associative: Boolean = true
    override val funOut: (GValue,Int) => GValue = {(v1,v2)=>/*v1*/ if (v2 > 0) v1 else GNull()}
    override val fun: (List[GValue]) => GValue = {
      (line) =>{/*val ss = line.map((gvalue) => {val s = gvalue.asInstanceOf[GDouble].v;s})*/
        val ss = line.flatMap((gvalue) => {if (gvalue.isInstanceOf[GDouble]) Some(gvalue.asInstanceOf[GDouble].v) else None})
        if(!ss.isEmpty){
        val dd = ss.reduce(_ + _);
        GDouble(dd)
        }else /*GDouble (0)*/ GNull()
      }
    }}

  private def getMin(position:Int, output_name:Option[String]) = new RegionsToRegion {
    override val resType = ParsingType.DOUBLE
    override val index: Int = position
    override val associative: Boolean = true
    override val funOut: (GValue,Int) => GValue = {(v1,v2)=>/*v1*/ if (v2 > 0) v1 else GNull()}
    override val fun: (List[GValue]) => GValue = {
      (line) =>
        /*val len = line.size.toDouble*/
        val lines = line.flatMap((gvalue) => if (gvalue.isInstanceOf[GDouble]) Some(gvalue.asInstanceOf[GDouble].v) else None)
        if(/*len != 0*/ !lines.isEmpty)
          GDouble(/*line.map((gvalue) => gvalue.asInstanceOf[GDouble].v)*/ lines.reduce( (x,y) =>Math.min(x,y)))
        else /*GDouble(0)*/ GNull()
    }}

  private def getMax(position:Int, output_name:Option[String]) = new RegionsToRegion {
    override val resType = ParsingType.DOUBLE
    override val index: Int = position
    override val associative: Boolean = true
    override val funOut: (GValue,Int) => GValue = {(v1,v2)=>/*v1*/ if (v2 > 0) v1 else GNull()}
    override val fun: (List[GValue]) => GValue = {
      (line) =>
        /*val len = line.size.toDouble*/
        val lines = line.flatMap((gvalue) => if (gvalue.isInstanceOf[GDouble]) Some(gvalue.asInstanceOf[GDouble].v) else None)
        if(/*len != 0*/ !lines.isEmpty)
        GDouble(/*line.map((gvalue) => gvalue.asInstanceOf[GDouble].v)*/ lines.reduce( (x,y) =>Math.max(x,y)))
        else /*GDouble(0)*/ GNull()
    }}

  private def getAvg(position:Int, output_name:Option[String]) = new RegionsToRegion {
    override val resType = ParsingType.DOUBLE
    override val index: Int = position
    override val associative: Boolean = true
    override val funOut: (GValue,Int) => GValue = {(v1,v2)=>/*GDouble(v1.asInstanceOf[GDouble].v/v2)*/
      if (v2 > 0 && !v1.isInstanceOf[GNull]) GDouble(v1.asInstanceOf[GDouble].v / v2) else GNull()}
    override val fun: (List[GValue]) => GValue = {
      (line) =>
        /*val len = line.size.toDouble*/
        val lines = line.flatMap((gvalue) => if (gvalue.isInstanceOf[GDouble]) Some(gvalue.asInstanceOf[GDouble].v) else None)
        if(/*len != 0*/ !lines.isEmpty)
          GDouble(/*(line.map((gvalue) => gvalue.asInstanceOf[GDouble].v).reduce(_ + _))*//*/len*/ (lines.reduce(_ + _)))
        else
          /*GDouble(0)*/ GNull()
    }}

  private def getBAG(position:Int, output_name:Option[String]) = new RegionsToRegion {
    override val resType = ParsingType.STRING
    override val index: Int = position
    override val associative: Boolean = true
    override val funOut: (GValue,Int) => GValue = {(v1,v2)=>/*v1*/ if (v2 > 0) v1 else GNull()}
    override val fun: (List[GValue]) => GValue = {
      (line) =>
        if(line.nonEmpty)
          GString((line.map((gvalue) => {
            gvalue match{
              case GString(v) => List(v)
              case GDouble(v) => List(v.toString)
              case GInt(v) => List(v.toString)
              case GNull() => List("_")
            }
          }).distinct.reduce((a, b) => a ++ b)).sorted.mkString(" ")) // TODO sorted is added only for comparation reason, we can get rid of it
        else
          GString(" ")

    }}


}
