package it.polimi.genomics.spark.implementation.RegionsOperators.SelectRegions

import it.polimi.genomics.core.DataTypes.GRECORD

/**
  * Created by abdulrahman kaitoua on 25/05/15.
  */
object StoreTABRD extends Store("gdm") {

  @transient var currentSize = 100

  override def toLine(record: GRECORD, startOffset: Int): String = {
    val newStart = record._1._3 + startOffset
    val stringBuilder = new StringBuilder(currentSize)
    stringBuilder
      .append(record._1._2)
      .append("\t")
      .append(newStart)
      .append("\t")
      .append(record._1._4)
      .append("\t")
      .append(record._1._5)
    record._2.foreach(stringBuilder.append("\t").append(_))

    if (currentSize < stringBuilder.capacity) {
      println(currentSize + "->" + stringBuilder.capacity)
      currentSize = stringBuilder.capacity
    }
    stringBuilder.toString()
  }


}
