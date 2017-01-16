package it.polimi.genomics.spark.test

import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.{GRecordKey, GValue}

/**
 * Created by Abdulrahman Kaitoua on 01/06/15.
 * Email: abdulrahman.kaitoua@polimi.it
 *
 */
object GOrdering {

  val metaOrderByID = new Ordering[MetaType](){override def compare (x:MetaType,y:MetaType):Int =x._1 compare y._1}
  val regionOrderByID = new Ordering[(GRecordKey,Array[GValue])]() {
    override def compare(x: (GRecordKey, Array[GValue]), y: (GRecordKey, Array[GValue])): Int = {
      x._1._1.compare(y._1._1)
    }
  }
}
