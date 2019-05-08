package it.polimi.genomics.spark.implementation.RegionsOperators

import com.google.common.hash.Hashing
import it.polimi.genomics.core.DataStructures.JoinParametersRD.RegionBuilder.RegionBuilder
import it.polimi.genomics.core.DataStructures.JoinParametersRD._
import it.polimi.genomics.core.DataStructures.RegionAggregate.COORD_POS
import it.polimi.genomics.core.DataStructures.{OptionalMetaJoinOperator, RegionOperator}
import it.polimi.genomics.core.DataTypes._
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.core.{GDouble, GRecordKey, GString, GValue}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import it.polimi.genomics.spark.implementation.RegionsOperators.GenometricMap.MapKey
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

object GenometricJoin {
  private final val logger = LoggerFactory.getLogger(this.getClass)

  @throws[SelectFormatException]
  def apply(executor: GMQLSparkExecutor,
            metajoinCondition: OptionalMetaJoinOperator,
            distanceJoinCondition: List[JoinQuadruple],
            regionBuilder: RegionBuilder,
            leftDataset: RegionOperator,
            rightDataset: RegionOperator,
            joinOnAttributes: Option[List[(Int, Int)]],
            BINNING_PARAMETER: Long,
            MAXIMUM_DISTANCE: Long,
            sc: SparkContext): RDD[GRECORD] = {
    implicit val orderGRECORD: Ordering[(GRecordKey, Array[GValue])] = Ordering.by { ar: GRECORD => ar._1 }

    // load datasets
    val ref: RDD[GRECORD] =
      executor.implement_rd(leftDataset, sc)
    val exp =
      executor.implement_rd(rightDataset, sc)

    val groups =
      executor
        .implement_mjd(metajoinCondition, sc)
        .flatMap {
          x => x._2.map(s => (x._1, s, Hashing.md5().newHasher().putLong(x._1).putLong(s).hash().asLong))
        }

    //Left -> List( (Right, newId), ..., ...  )
    val refGroups: Map[Long, Iterable[(Long, Long)]] =
      sc.broadcast(groups.map(x => (x._1, (x._2, x._3))).groupByKey().collectAsMap()).value.toMap

    val left_before_size = refGroups.size
    val right_size = refGroups.values.map(x => x.map(_._1).toSet).reduce(_ ++ _).size
    val left_new_size = refGroups.values.map(x => x.map(_._2).toSet).reduce(_ ++ _).size

    val output: RDD[(GRecordKey, Array[GValue])] = {

      // (ExpID,chr ,bin), start, stop, strand, values,BinStart)
      distanceJoinCondition.map { q =>
        val qList = q.toList()

        val firstRoundParameters: JoinExecutionParameter =
          createExecutionParameters(qList.takeWhile(!_.isInstanceOf[MinDistance]))
        val remaining: List[AtomicCondition] =
          qList.dropWhile(!_.isInstanceOf[MinDistance])
        val minDistanceParameter: Option[AtomicCondition] =
          remaining.headOption
        val secondRoundParameters: JoinExecutionParameter =
          createExecutionParameters(remaining.drop(1))


        val maxDistance: Long =
          if (firstRoundParameters.max.isDefined) Math.max(0L, firstRoundParameters.max.get)
          else if (secondRoundParameters.max.isDefined) Math.max(secondRoundParameters.max.get, MAXIMUM_DISTANCE)
          else if (firstRoundParameters.min.isDefined) firstRoundParameters.min.get + MAXIMUM_DISTANCE
          else MAXIMUM_DISTANCE

        val repartitionConstant = 4 * Math.ceil(2.0 * maxDistance / BINNING_PARAMETER).toInt

        //(idRight, bin, chrom), (gRecordKey, values, newId)
        val binnedRef = binLeftDs(
          ref.repartition(left_new_size * repartitionConstant),
          refGroups,
          firstRoundParameters,
          secondRoundParameters,
          BINNING_PARAMETER,
          maxDistance)

        //(idRight, bin, chrom), (gRecordKey, values)
        val binnedRight = binRightDs(exp.repartition(right_size * repartitionConstant), BINNING_PARAMETER)

        val joined2: RDD[((Long, Int, String), ((GRecordKey, Array[GValue], Long), (GRecordKey, Array[GValue])))] =
          binnedRef
            .join(binnedRight)

        def getValue(index: Int, region: GRecordKey, values: Array[GValue]) = index match {
          case COORD_POS.CHR_POS => region.chrom
          case COORD_POS.LEFT_POS => region.start
          case COORD_POS.RIGHT_POS => region.stop
          case COORD_POS.START_POS => if (region.strand == '-') region.stop else region.start
          case COORD_POS.STOP_POS => if (region.strand == '-') region.start else region.stop
          case COORD_POS.STRAND_POS => region.strand
          case ind => values(ind)
        }

        val filteredAttribute: RDD[((Long, Int, String), ((GRecordKey, Array[GValue], Long), (GRecordKey, Array[GValue])))] =
          if (joinOnAttributes.getOrElse(List.empty).nonEmpty) {
            joined2.filter {
              case (_, ((gRKLeft, valuesLeft, newId), (gRKRight, valuesRight))) =>
                joinOnAttributes.getOrElse(List.empty)
                  .foldLeft(true) {
                    case (pre, (in1, in2)) =>
                      pre && getValue(in1, gRKLeft, valuesLeft) == getValue(in2, gRKRight, valuesRight)
                  }
            }
          }
          else
            joined2


        val filteredRegions =
          if (distanceJoinCondition.nonEmpty) {
            filteredAttribute
          }
          else
            filteredAttribute


        val first =
          filteredAttribute
            .filter { case ((_, bin, _), ((gRKL, _, _), (gRKR, _))) =>
              val startRefBin = computeBinStartRef(gRKL, firstRoundParameters, secondRoundParameters, maxDistance, BINNING_PARAMETER)
              val expStartBin = (gRKR.start / BINNING_PARAMETER).toInt
              (bin == startRefBin || bin == expStartBin) &&
                (gRKL.strand.equals('*') || gRKR.strand.equals('*') || gRKL.strand.equals(gRKR.strand))
            }
            .map(_._2)
            .filter { case ((gRKL, valuesL, newId), (gRKR, valuesR)) =>
              val r = gRKL
              val e = gRKR

              val distance: Long = distanceCalculator((r.start, r.stop), (e.start, e.stop))

              val intersect_distance =
                (firstRoundParameters.max.isEmpty || firstRoundParameters.max.get > distance) &&
                  (firstRoundParameters.min.isEmpty || firstRoundParameters.min.get < distance)
              val no_stream = firstRoundParameters.stream.isEmpty
              val UPSTREAM = if (no_stream) true
              else (
                firstRoundParameters.stream.get.equals('+') // upstream
                  &&
                  (
                    ((r.strand.equals('+') || r.strand.equals('*')) && e.stop <= r.start) // reference with positive strand =>  experiment must be earlier
                      ||
                      (r.strand.equals('-') && e.start >= r.stop) // reference with negative strand => experiment must be later
                    )
                )
              val DOWNSTREAM = if (no_stream) true
              else
                (
                  firstRoundParameters.stream.get.equals('-') // downstream
                    &&
                    (
                      ((r.strand.equals('+') || r.strand.equals('*')) && e.start >= r.stop) // reference with positive strand =>  experiment must be later
                        ||
                        (r.strand.equals('-') && e.stop <= r.start) // reference with negative strand => experiment must be earlier
                      )
                  )
              intersect_distance && (no_stream || UPSTREAM || DOWNSTREAM)
            }


        val firstRound: RDD[((GRecordKey, Array[GValue]), (GRecordKey, Array[GValue]))] = if (minDistanceParameter.isDefined) {

          first.map { case ((gRKL, valuesL, newId), (gRKR, valuesR)) =>
            (MapKey(newId, gRKL.chrom, gRKL.start, gRKL.stop, gRKL.strand, valuesL.toList), (gRKR, valuesR))
          }
            .groupByKey()
            .flatMap { case (mapKey, iter) =>

              val itr = iter
                .toList
                .map(e =>
                  (e, distanceCalculator((mapKey.refStart, mapKey.refStop), (e._1.start, e._1.stop))))
                .sortBy(_._2)
              val count = Math.min(itr.length, minDistanceParameter.get.asInstanceOf[MinDistance].number)
              itr
                .filter(_._2 <= itr(count - 1)._2)
                .map(s =>
                  ((GRecordKey(mapKey.newId, mapKey.refChr, mapKey.refStart, mapKey.refStop, mapKey.refStrand), mapKey.refValues.toArray), s._1)
                )
            }
        }
        else
          first.map((x: ((GRecordKey, Array[GValue], Long), (GRecordKey, Array[GValue]))) => ((x._1._1, x._1._2), x._2))

        val res_pairs: RDD[((GRecordKey, Array[GValue]), (GRecordKey, Array[GValue]))] = if (
          secondRoundParameters.max.isDefined ||
            secondRoundParameters.min.isDefined ||
            secondRoundParameters.stream.isDefined) {

          firstRound.filter {
            case ((gRKL, _), (gRKR, _)) =>
              val distance = distanceCalculator((gRKL.start, gRKL.stop), (gRKR.start, gRKR.stop))
              (secondRoundParameters.max.isEmpty || secondRoundParameters.max.get > distance) &&
                (secondRoundParameters.min.isEmpty || secondRoundParameters.min.get < distance) &&
                (secondRoundParameters.stream.isEmpty ||
                  (secondRoundParameters.stream.get.equals('+') && (
                    ((gRKL.strand.equals('+') || gRKL.strand.equals('*')) && gRKR.stop <= gRKL.start) // reference with positive strand =>  experiment must be earlier
                      ||
                      (gRKL.strand.equals('-') && gRKR.start >= gRKL.stop) // reference with negative strand => experiment must be later
                    )) || (
                  secondRoundParameters.stream.get.equals('-') // downstream
                    &&
                    (
                      ((gRKL.strand.equals('+') || gRKL.strand.equals('*')) && gRKR.start >= gRKL.stop) // reference with positive strand =>  experiment must be later
                        ||
                        (gRKL.strand.equals('-') && gRKR.stop <= gRKL.start) // reference with negative strand => experiment must be earlier
                      )))
          }

        } else {
          firstRound
        }

        val res_pairs_filtd =
          if (regionBuilder == RegionBuilder.INTERSECTION) {
            res_pairs
              .filter {
                case ((gRKL, _), (gRKR, _)) => gRKL.start < gRKR.stop && gRKR.start < gRKL.stop
              }
          }
          else
            res_pairs

        res_pairs_filtd.map(x => joinRegions(x._1, x._2, regionBuilder))

      }.reduce { (a: RDD[GRECORD], b: RDD[GRECORD]) => a.union(b) }
    }

    val distinct_output = regionBuilder match {
      case RegionBuilder.RIGHT_DISTINCT => distinct(output)
      case RegionBuilder.LEFT_DISTINCT => distinct(output)
      case _ => output
    }


    distinct_output
  }

  def distinct(ds: RDD[GRECORD]): RDD[(GRecordKey, Array[GValue])] = {
    implicit val order = Ordering.by { x: (GRecordKey, Array[GValue]) => x._1 + x._2.mkString(",") }
    ds
      .groupBy(x => (x._1, x._2.deep))
      .flatMap { s =>
        val set = s._2.toList.sorted
        var buf = set.head
        if (set.size > 1) buf :: set.tail.flatMap(record => if (buf._2.deep == record._2.deep) None else {
          buf = record
          Some(record)
        })
        else set
      }
  }


  //(idRight, bin, chrom), (gRecordKey, values)
  def binRightDs(ds: RDD[GRECORD], binSize: Long): RDD[((Long, Int, String), (GRecordKey, Array[GValue]))] =
    ds.flatMap {
      case (gRecordKey, values) =>
        val startBin = (gRecordKey.start / binSize).toInt
        val stopBin = (gRecordKey.stop / binSize).toInt

        (startBin to stopBin).map(bin => ((gRecordKey.id, bin, gRecordKey.chrom), (gRecordKey, values)))
    }

  def computeBinStartRef(rKey: GRecordKey, firstRound: JoinExecutionParameter, secondRound: JoinExecutionParameter, maxDistance: Long, binSize: Long) =
    (
      Math.max(
        0L,
        if (firstRound.stream.isEmpty ||
          firstRound.stream.get.equals(rKey.strand) ||
          (rKey.strand.equals('*') && firstRound.stream.get.equals('+'))
        )
          rKey.start - maxDistance
        else
          rKey.stop
      ) / binSize
      ).toInt

  def computeBinStopRef(rKey: GRecordKey, firstRound: JoinExecutionParameter, secondRound: JoinExecutionParameter, maxDistance: Long, binSize: Long) =
    (
      (
        if (firstRound.stream.isEmpty ||
          !firstRound.stream.get.equals(rKey.strand) ||
          (rKey.strand.equals('*') && firstRound.stream.get.equals('-'))) {
          rKey.stop + maxDistance
        }
        else
          rKey.start
        ) / binSize
      ).toInt

  def binLeftDs(ds: RDD[GRECORD],
                refGroups: Map[Long, Iterable[(Long, Long)]],
                firstRound: JoinExecutionParameter,
                secondRound: JoinExecutionParameter,
                binSize: Long,
                maxDistance: Long
               ): RDD[((Long, Int, String), (GRecordKey, Array[GValue], Long))] = {
    ds
      .flatMap {
        case (rKey: GRecordKey, values: Array[GValue]) =>
          val binStart = computeBinStartRef(rKey, firstRound, secondRound, maxDistance, binSize)
          val binEnd = computeBinStopRef(rKey, firstRound, secondRound, maxDistance, binSize)

          for (newId <- refGroups.getOrElse(rKey.id, List.empty); bin <- binStart to binEnd) yield
            ((newId._1, bin, rKey.chrom), (rKey, values, newId._2))
      }

  }


  def joinRegions(left: GRECORD, right: GRECORD, regionBuilder: RegionBuilder) = {
    regionBuilder match {
      case RegionBuilder.LEFT => (left._1, left._2 ++ right._2)
      case RegionBuilder.LEFT_DISTINCT => left
      case RegionBuilder.RIGHT => (GRecordKey(left._1.id, right._1.chrom, right._1.start, right._1.stop, right._1.strand), left._2 ++ right._2)
      case RegionBuilder.RIGHT_DISTINCT => (GRecordKey(left._1.id, right._1.chrom, right._1.start, right._1.stop, right._1.strand), right._2)
      case RegionBuilder.CONTIG => (
        GRecordKey(
          left._1.id,
          left._1.chrom,
          Math.min(left._1.start, right._1.start),
          Math.max(left._1.stop, right._1.stop),
          left._1.strand),
        left._2 ++ right._2)
      case RegionBuilder.BOTH => (
        left._1,
        left._2 ++ Array[GValue](GString(right._1.chrom), GDouble(right._1.start), GDouble(right._1.stop), GString(right._1.strand.toString)) ++ right._2)
      case RegionBuilder.INTERSECTION => (
        GRecordKey(
          left._1.id,
          left._1.chrom,
          Math.max(left._1.start, right._1.start),
          Math.min(left._1.stop, right._1.stop),
          if (left._1.strand.equals(right._1.strand)) left._1.strand else '*'),
        left._2 ++ right._2
      )
    }
  }


  def distanceCalculator(a: (Long, Long), b: (Long, Long)): Long = {

    val d1: Long = a._1 - b._2
    val d2: Long = b._1 - a._2

    if (a._2 < b._1 || b._2 < a._1) {
      Math.min(Math.abs(d1), Math.abs(d2))
    } else {
      -Math.min(Math.abs(d1), Math.abs(d2))
    }

  }

  def createExecutionParameters(list: List[AtomicCondition]): JoinExecutionParameter = {
    def helper(list: List[AtomicCondition], temp: JoinExecutionParameter): JoinExecutionParameter = {
      if (list.isEmpty) {
        temp
      } else {
        val current = list.head
        current match {
          case DistLess(v) => helper(list.tail, new JoinExecutionParameter(Some(v), temp.min, temp.stream))
          case DistGreater(v) => helper(list.tail, new JoinExecutionParameter(temp.max, Some(v), temp.stream))
          case Upstream() => helper(list.tail, new JoinExecutionParameter(temp.max, temp.min, Some('+')))
          case DownStream() => helper(list.tail, new JoinExecutionParameter(temp.max, temp.min, Some('-')))
        }
      }
    }

    helper(list, new JoinExecutionParameter(None, None, None))
  }

  class JoinExecutionParameter(val max: Option[Long], val min: Option[Long], val stream: Option[Char]) extends Serializable {
    override def toString() = {
      "JoinParam max:" + {
        if (max.isDefined) {
          max.get
        }
      } + " min: " + {
        if (min.isDefined) {
          min.get
        }
      } + " stream: " + {
        if (stream.isDefined) {
          stream.get
        }
      }
    }
  }

}
