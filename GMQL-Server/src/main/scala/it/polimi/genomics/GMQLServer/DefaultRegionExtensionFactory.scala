package it.polimi.genomics.GMQLServer

import it.polimi.genomics.core.DataStructures.Builtin.RegionExtensionFactory
import it.polimi.genomics.core.DataStructures.RegionAggregate._
import it.polimi.genomics.core._

/**
  * Created by pietro on 04/05/16.
  */
object DefaultRegionExtensionFactory extends RegionExtensionFactory{

  override def get(dag : RENode, output : Either[String, Int]) : RegionExtension = {

    val indexes = extract_indexes(dag).toList

    new RegionExtension {
      override val fun: (Array[GValue]) => GValue = make_fun(dag,indexes)
      override val inputIndexes: List[Int] = indexes
      override val output_index = output match {
        case Right(p) => Some(p)
        case _ => None
      }
      override val output_name = output match {
        case Left(n) => Some(n)
        case _ => None
      }

      override val out_type = dag match {
        case REStringConstant(_) => ParsingType.STRING
        case _ => ParsingType.DOUBLE
      }
    }
  }

  def extract_indexes(dag : RENode) : Set[Int] = {
    dag match {
      case RESTART() => Set(COORD_POS.LEFT_POS,COORD_POS.RIGHT_POS,COORD_POS.STRAND_POS)
      case RESTOP() => Set(COORD_POS.LEFT_POS,COORD_POS.RIGHT_POS,COORD_POS.STRAND_POS)
      case REPos(x) => Set(x)
      case READD(x,y) => extract_indexes(x) ++ extract_indexes(y)
      case RESUB(x,y) => extract_indexes(x) ++ extract_indexes(y)
      case REMUL(x,y) => extract_indexes(x) ++ extract_indexes(y)
      case REDIV(x,y) => extract_indexes(x) ++ extract_indexes(y)
      case RECHR() => Set(COORD_POS.CHR_POS)
      case RELEFT() => Set(COORD_POS.LEFT_POS)
      case RERIGHT() => Set(COORD_POS.RIGHT_POS)
      case RESTRAND() => Set(COORD_POS.STRAND_POS)
      case _ => Set.empty
    }
  }

  def make_fun(node : RENode, indexes : List[Int]) : (Array[GValue] => GValue) = {
    node match {
      case REPos(p) => (x:Array[GValue]) => x(indexes.indexOf(p))
      case RELEFT() => (x:Array[GValue]) => x(indexes.indexOf(COORD_POS.LEFT_POS))
      case RERIGHT() => (x:Array[GValue]) => x(indexes.indexOf(COORD_POS.RIGHT_POS))
      case RESTRAND() => (x:Array[GValue]) => x(indexes.indexOf(COORD_POS.STRAND_POS))
      case RECHR() => (x:Array[GValue]) => x(indexes.indexOf(COORD_POS.CHR_POS))
      case RESTART() => (x:Array[GValue]) => {
        val strand = x(indexes.indexOf(COORD_POS.STRAND_POS)).asInstanceOf[GString].v
        if (strand.equals("-")) x(indexes.indexOf(COORD_POS.RIGHT_POS)) else x(indexes.indexOf(COORD_POS.LEFT_POS))
      }
      case RESTOP() => (x:Array[GValue]) => {
        val strand = x(indexes.indexOf(COORD_POS.STRAND_POS)).asInstanceOf[GString].v
        if (strand.equals("-")) x(indexes.indexOf(COORD_POS.LEFT_POS)) else x(indexes.indexOf(COORD_POS.RIGHT_POS))
      }
      case READD(a,b) => (x:Array[GValue]) => {
        /*GDouble(
          make_fun(a,indexes)(x).asInstanceOf[GDouble].v +
            make_fun(b,indexes)(x).asInstanceOf[GDouble].v)*/
        if (make_fun(a, indexes)(x).isInstanceOf[GNull] || make_fun(b, indexes)(x).isInstanceOf[GNull])
          GNull()
        else {
          //strand is present in indexes only when changing start or stop
          if (indexes.indexOf(COORD_POS.STRAND_POS) > -1) {
            val strand = x(indexes.indexOf(COORD_POS.STRAND_POS)).asInstanceOf[GString].v
            if (strand.equals("-")) GDouble(
              make_fun(a, indexes)(x).asInstanceOf[GDouble].v -
                make_fun(b, indexes)(x).asInstanceOf[GDouble].v)
            else GDouble(
              make_fun(a, indexes)(x).asInstanceOf[GDouble].v +
                make_fun(b, indexes)(x).asInstanceOf[GDouble].v)
          }
          else GDouble(
            make_fun(a, indexes)(x).asInstanceOf[GDouble].v +
              make_fun(b, indexes)(x).asInstanceOf[GDouble].v)
        }
      }
      case RESUB(a,b) => (x:Array[GValue]) => {
        /*GDouble(
          make_fun(a,indexes)(x).asInstanceOf[GDouble].v -
            make_fun(b,indexes)(x).asInstanceOf[GDouble].v)*/
        if (make_fun(a, indexes)(x).isInstanceOf[GNull] || make_fun(b, indexes)(x).isInstanceOf[GNull])
          GNull()
        else {
          //strand is present in indexes only when changing start or stop
          if (indexes.indexOf(COORD_POS.STRAND_POS) > -1) {
            val strand = x(indexes.indexOf(COORD_POS.STRAND_POS)).asInstanceOf[GString].v
            if (strand.equals("-")) GDouble(
              make_fun(a, indexes)(x).asInstanceOf[GDouble].v +
                make_fun(b, indexes)(x).asInstanceOf[GDouble].v)
            else GDouble(
              make_fun(a, indexes)(x).asInstanceOf[GDouble].v -
                make_fun(b, indexes)(x).asInstanceOf[GDouble].v)
          }
          else GDouble(
            make_fun(a, indexes)(x).asInstanceOf[GDouble].v -
              make_fun(b, indexes)(x).asInstanceOf[GDouble].v)
        }
      }
      case REMUL(a,b) => (x:Array[GValue]) => {
       /* GDouble(
          make_fun(a,indexes)(x).asInstanceOf[GDouble].v *
            make_fun(b,indexes)(x).asInstanceOf[GDouble].v)*/
        if (make_fun(a,indexes)(x).isInstanceOf[GNull] || make_fun(b,indexes)(x).isInstanceOf[GNull])
          GNull()
        else
          GDouble(make_fun(a,indexes)(x).asInstanceOf[GDouble].v * make_fun(b,indexes)(x).asInstanceOf[GDouble].v)
      }
      case REDIV(a,b) => (x:Array[GValue]) => {
        /*GDouble(
          make_fun(a,indexes)(x).asInstanceOf[GDouble].v /
            make_fun(b,indexes)(x).asInstanceOf[GDouble].v)*/
        if (make_fun(a,indexes)(x).isInstanceOf[GNull] || make_fun(b,indexes)(x).isInstanceOf[GNull])
          GNull()
        else
          GDouble(make_fun(a,indexes)(x).asInstanceOf[GDouble].v / make_fun(b,indexes)(x).asInstanceOf[GDouble].v)
      }
      case REFloat(f) => (x:Array[GValue]) => GDouble(f)
      case REStringConstant(c) => { (x:Array[GValue]) => GString(c)}
    }
  }
}
