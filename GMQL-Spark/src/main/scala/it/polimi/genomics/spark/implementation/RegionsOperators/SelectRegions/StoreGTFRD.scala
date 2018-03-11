package it.polimi.genomics.spark.implementation.RegionsOperators.SelectRegions

import it.polimi.genomics.core.DataStructures.{MetaOperator, RegionOperator}
import it.polimi.genomics.core.DataTypes.GRECORD
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.core.{GMQLSchemaCoordinateSystem, GNull, GValue}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by abdulrahman kaitoua on 25/05/15.
  */


class StoreGTFRD(schema: List[(String, PARSING_TYPE)]) extends Store("gtf")  {

  @transient var currentSize = 100


  @transient lazy val (
    scoreIndex: Int,
    sourceIndex: Int,
    featureIndex: Int,
    frameIndex: Int,
    schemaOthers: List[String]) = {
    val schemaZipped = schema.map(_._1.toLowerCase).zipWithIndex

    def getPosition(columnName: String) = schemaZipped.find(_._1 == columnName).map(_._2).getOrElse(-1)

    val scoreIndex = getPosition("score")
    val sourceIndex = getPosition("source")
    val featureIndex = getPosition("feature")
    val frameIndex = getPosition("frame")

    val schemaOthers = schemaZipped
      .filter {
        case (_, index: Int) =>
          index != scoreIndex && index != sourceIndex && index != featureIndex && index != frameIndex
      }
      .map(_._1)
    (scoreIndex, sourceIndex, featureIndex, frameIndex, schemaOthers)
  }

  override def toLine(record: GRECORD, startOffset: Int): String = {
    val values = record._2.iterator.zipWithIndex
      .filter {
        case (_, index: Int) =>
          index != scoreIndex && index != sourceIndex && index != featureIndex && index != frameIndex
      }
      .map(_._1).zip(schemaOthers.iterator)

    val newStart = record._1._3 + startOffset
    val strand = record._1._5
    val stringBuilder = new StringBuilder(currentSize)
    stringBuilder
      .append(record._1._2)
      .append("\t").append(if (sourceIndex >= 0) record._2(sourceIndex).toString else "GMQL")
      .append("\t").append(if (featureIndex >= 0) record._2(featureIndex) else "Region")
      .append("\t").append(newStart)
      .append("\t").append(record._1._4)
      .append("\t").append(if (scoreIndex >= 0) record._2(scoreIndex) else "0.0")
      .append("\t").append(if (strand.equals('*')) '.' else strand)
      .append("\t").append(if (frameIndex >= 0) record._2(frameIndex) else ".")
      .append("\t")
    if (values.nonEmpty)
      values.foreach {
        case (_: GNull, _) =>
        case (value: GValue, name: String) =>
          stringBuilder.append(name).append(" \"").append(value).append("\";")
      }

    if (currentSize < stringBuilder.capacity)
      currentSize = stringBuilder.capacity
    stringBuilder.toString()
  }
}


object StoreGTFRD  {
  final val extension = new StoreGTFRD(null).extension

  @throws[SelectFormatException]
  def apply(executor: GMQLSparkExecutor, path: String, value: RegionOperator, associatedMeta: MetaOperator, schema: List[(String, PARSING_TYPE)], coordinateSystem: GMQLSchemaCoordinateSystem.Value, sc: SparkContext): RDD[GRECORD] =
    new StoreGTFRD(schema).apply(executor, path, value, associatedMeta, schema, coordinateSystem, sc)

}
