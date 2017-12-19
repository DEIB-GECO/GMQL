package it.polimi.genomics.spark.implementation.RegionsOperators


import it.polimi.genomics.core.DataStructures.RegionAggregate.{COORD_POS, RegionExtension}
import it.polimi.genomics.core.DataStructures.RegionCondition.MetaAccessor
import it.polimi.genomics.core.DataStructures.{MetaOperator, RegionOperator}
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.core._
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import it.polimi.genomics.spark.implementation.RegionsOperators.PredicateRD.executor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
  * Created by abdulrahman kaitoua on 06/07/15.
  */
object ProjectRD {
  private final val logger = LoggerFactory.getLogger(this.getClass);

  @throws[SelectFormatException]
  def apply(executor: GMQLSparkExecutor,
            projectedValues: Option[List[Int]],
            tupleAggregator: Option[List[RegionExtension]],
            inputDataset: RegionOperator,
            inputMeta: MetaOperator,
            env: SparkContext): RDD[GRECORD] = {
    logger.info("----------------ProjectRD executing..")

    val input = executor.implement_rd(inputDataset, env)
    val metadata = executor.implement_md(inputMeta, env)


    //metadata pairs needed
    val meta: Array[(Long, (String, String))] =
      if (tupleAggregator.isDefined) {
        val metaaccessors = tupleAggregator.get.flatMap {
          agg =>
            agg.inputIndexes.flatMap(
              in =>
                if (in.isInstanceOf[MetaAccessor])
                  Some(in.asInstanceOf[MetaAccessor].attribute_name)
                else
                  None
            )
        }
        if (metaaccessors.size > 0)
          metadata
            .filter(x => metaaccessors.contains(x._2._1))
            .distinct
            .collect
        else
          Array[(Long, (String, String))]()
      }
      else
        Array[(Long, (String, String))]()

    val chr_fun = try {
      Some(
        tupleAggregator
          .getOrElse(List.empty)
          .filter(_.output_index == Some(COORD_POS.CHR_POS))(0)
      )
    } catch {
      case _ => None
    }

    val strand_fun = try {
      Some(
        tupleAggregator
          .getOrElse(List.empty)
          .filter(_.output_index == Some(COORD_POS.STRAND_POS))(0)
      )
    } catch {
      case _ => None
    }

    val left_fun = try {
      Some(
        tupleAggregator
          .getOrElse(List.empty)
          .filter(_.output_index == Some(COORD_POS.LEFT_POS))(0)
      )
    } catch {
      case _ => None
    }

    val right_fun = try {
      Some(
        tupleAggregator
          .getOrElse(List.empty)
          .filter(_.output_index == Some(COORD_POS.RIGHT_POS))(0)
      )
    } catch {
      case _ => None
    }

    val start_fun = try {
      Some(
        tupleAggregator
          .getOrElse(List.empty)
          .filter(_.output_index == Some(COORD_POS.START_POS))(0)
      )
    } catch {
      case _ => None
    }

    val stop_fun = try {
      Some(
        tupleAggregator
          .getOrElse(List.empty)
          .filter(_.output_index == Some(COORD_POS.STOP_POS))(0)
      )
    } catch {
      case _ => None
    }

    val modifier_funs =
      tupleAggregator
        .getOrElse(List.empty)
        .filter(x => x.output_index.isDefined && x.output_index.get >= 0)

    val new_fields_funs =
      tupleAggregator
        .getOrElse(List.empty)
        .filter(!_.output_index.isDefined)


    if (tupleAggregator.isDefined) {
      input
        .flatMap(
          modify_tuple(
            _,
            meta,
            chr_fun,
            strand_fun,
            left_fun,
            right_fun,
            start_fun,
            stop_fun,
            modifier_funs,
            new_fields_funs,
            projectedValues))
    } else {
      input.map(
        a => (
          a._1, projectedValues.get.foldLeft(Array[GValue]())((Acc, b) => Acc :+ a._2(b))
        ))
    }

  }


  def computeFunction(r: GRECORD, agg: RegionExtension, inputMeta: Array[(Long, (String, String))]): GValue = {
    agg
      .fun(
        agg.inputIndexes.foldLeft(Array[GValue]())((acc, b) => acc :+ {
          if (b.isInstanceOf[MetaAccessor]) {
            val values = inputMeta.filter(x =>
              x._1 == r._1._1 &&
                x._2._1.equals(b.asInstanceOf[MetaAccessor].attribute_name))

            if (values.size > 0) {
              val value = values.head._2._2
              try {
                GDouble(value.toDouble)
              }
              catch {
                case _: Throwable => GString(value)
              }
            } else GNull()
          } else
            b.asInstanceOf[Int] match {
              case COORD_POS.CHR_POS => new GString(r._1._2)
              case COORD_POS.LEFT_POS => new GDouble(r._1._3)
              case COORD_POS.RIGHT_POS => new GDouble(r._1._4)
              case COORD_POS.STRAND_POS => new GString(r._1._5.toString)
              case _: Int => r._2(b.asInstanceOf[Int])
            }
        }))
  }

  def modify_tuple(r: GRECORD,
                   inputMeta: Array[(Long, (String, String))],
                   chr_fun: Option[RegionExtension] = None,
                   strand_fun: Option[RegionExtension] = None,
                   left_fun: Option[RegionExtension] = None,
                   right_fun: Option[RegionExtension] = None,
                   start_fun: Option[RegionExtension] = None,
                   stop_fun: Option[RegionExtension] = None,
                   modify_existing: List[RegionExtension] = List.empty,
                   new_fields: List[RegionExtension] = List.empty,
                   projected_values: Option[List[Int]] = None
                  ): Option[GRECORD] = {

    val new_chr: String = if (chr_fun.isDefined) {
      computeFunction(r, chr_fun.get, inputMeta).asInstanceOf[GString].v
    } else {
      r._1.chrom
    }
    val new_strand: Char = if (strand_fun.isDefined) {
      computeFunction(r, strand_fun.get, inputMeta).asInstanceOf[GString].v.charAt(0)
    } else {
      r._1.strand
    }
    val new_left: Long = if (left_fun.isDefined) {
      math.max(0L, computeFunction(r, left_fun.get, inputMeta).asInstanceOf[GDouble].v.toLong)
    } else if (start_fun.isDefined && r._1.strand != '-') {
      math.max(0L, computeFunction(r, start_fun.get, inputMeta).asInstanceOf[GDouble].v.toLong)
    } else if (stop_fun.isDefined && r._1.strand == '-') {
      math.max(0L, computeFunction(r, stop_fun.get, inputMeta).asInstanceOf[GDouble].v.toLong)
    } else {
      r._1.start
    }
    val new_right: Long = if (right_fun.isDefined) {
      computeFunction(r, right_fun.get, inputMeta).asInstanceOf[GDouble].v.toLong
    } else if (start_fun.isDefined && r._1.strand == '-') {
      computeFunction(r, start_fun.get, inputMeta).asInstanceOf[GDouble].v.toLong
    } else if (stop_fun.isDefined && r._1.strand != '-') {
      computeFunction(r, stop_fun.get, inputMeta).asInstanceOf[GDouble].v.toLong
    } else {
      r._1.stop
    }

    if (
      (left_fun.isDefined || right_fun.isDefined || start_fun.isDefined || stop_fun.isDefined) &&
        (new_right <= new_left)) {
      return None
    }

    val attributes = r._2
    for (f <- modify_existing) {
      attributes.update(f.output_index.get, computeFunction(r, f, inputMeta))
    }

    val new_values = s(for (f <- new_fields) yield computeFunction(r, f, inputMeta)).toArray

    val all_attributes = attributes ++ new_values

    val out_attributes = if (projected_values.isDefined) {
      (for (i <- projected_values.get) yield all_attributes(i)).toArray
    } else {
      all_attributes
    }
    Some((new GRecordKey(r._1.id, new_chr, new_left, new_right, new_strand), out_attributes))
  }
}
