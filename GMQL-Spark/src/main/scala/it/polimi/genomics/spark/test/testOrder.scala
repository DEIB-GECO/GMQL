//package it.polimi.genomics.spark.Run
//
///**
// * Created by abdulrahman on 06/10/15.
// */
//
//import java.io.FileWriter
//import java.nio.charset.StandardCharsets
//import java.nio.file.{Files, Paths}
//
//import it.polimi.genomics.core.DataStructures.GroupMDParameters.Direction
//import it.polimi.genomics.core.{GString, GDouble, GValue}
//
//object testOrder {
//  def main(args: Array[String]): Unit = {
//
////    implicit val GvalueOrder :Ordering[Array[GValue]] = Ordering.by(x=>x.)
//
////    implicit object GvalueOrder extends Ordering[Array[GValue]] {
////    def compare(a:Array[GValue],b:Array[GValue]): Int ={
////      import scala.math.Ordered._
////      implicit val ord = Ordering.Tuple5[Int, Int, Int, Int, Int]( ) compare ()
////    }
////    }
//    val s: Array[GValue] = Array(GDouble(0.6).asInstanceOf[GValue],GString("a").asInstanceOf[GValue])
//
//    val data: Array[(Int, Array[GValue])] = Array((1,Array(GDouble(0.5).asInstanceOf[GValue],GString("a").asInstanceOf[GValue]))
//      ,(2,Array(GDouble(0.6).asInstanceOf[GValue],GString("a").asInstanceOf[GValue])),(3,Array(GDouble(0.5).asInstanceOf[GValue],GString("b").asInstanceOf[GValue])))
//
//    val ordering = List((0,Direction.ASC),(1,Direction.DESC))
//
//    val valuesOrdering = ordering.map(x=>  x._2 match {
//      case Direction.ASC => println (x);orderByColumn(x._1)
//      case Direction.DESC => println (x);orderByColumn(x._1).reverse
//    } ).reduceLeft((res,x)=>res orElse x)
//
//    val d = data.sortBy(x=>x._2)(valuesOrdering)
//    d.foreach(x=>println(x._1,x._2.mkString("\t")))
//  }
//  def orderByColumn(col: Int) = Ordering.by { ar: Array[GValue] => ar(col) }
//
//  final class CompositeOrdering[T]( val ord1: Ordering[T], val ord2: Ordering[T] ) extends Ordering[T] {
//    def compare( x: T, y: T ) = {
//      val comp = ord1.compare( x, y )
//      if ( comp != 0 ) comp else ord2.compare( x, y )
//    }
//  }
//  object CompositeOrdering {
//    def apply[T]( orderings: Ordering[T] * ) = orderings reduceLeft (_ orElse _)
//  }
//
//  implicit class OrderingOps[T]( val ord: Ordering[T] ) extends AnyVal {
//    def orElse( ord2: Ordering[T] ) = new CompositeOrdering[T]( ord, ord2 )
//  }
//}
