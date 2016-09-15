package it.polimi.genomics.flink.FlinkImplementation.operator.meta

import it.polimi.genomics.core.DataStructures.{MetaJoinOperator, MetaOperator}
import it.polimi.genomics.core.DataTypes.{FlinkMetaJoinType2, FlinkMetaType}
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import org.apache.flink.api.scala._
import org.apache.flink.hadoop.shaded.com.google.common.base.Charsets
import org.apache.flink.hadoop.shaded.com.google.common.hash.Hashing
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
 * Created by michelebertoni on 13/05/15.
 */
object CombineMD2{
  //val hf : HashFunction = Hashing.sha256()

  final val logger = LoggerFactory.getLogger(this.getClass)

  @throws[SelectFormatException]
  def apply(executor : FlinkImplementation, grouping : Option[MetaJoinOperator], leftDataset : MetaOperator, rightDataset : MetaOperator, env : ExecutionEnvironment) : DataSet[FlinkMetaType] = {
    //logger.warn("Executing CombineMD2")
    val left = executor.implement_md(leftDataset, env)
    val right = executor.implement_md(rightDataset, env)

    if (grouping.isDefined) {
      val pairs = extractPairs(executor.implement_mjd2(grouping.get, env)).collect()

      val leftOut = left.flatMap((l, out: Collector[FlinkMetaType]) => {
        pairs.map((p) => {
          if(l._1.equals(p._1)) {
            out.collect((Hashing.md5().hashString((l._1.toString + p.toString).toString, Charsets.UTF_8).asLong(), l._2, l._3))
          }
        })
      })

      val rightOut = right.flatMap((r, out: Collector[FlinkMetaType]) => {
        pairs.map((p) => {
          if(r._1.equals(p._1)) {
            out.collect((Hashing.md5().hashString((p.toString + r._1.toString).toString, Charsets.UTF_8).asLong(), r._2, r._3))
          }
        })
      })

      leftOut.union(rightOut)


    } else {
      val leftIds = left.map(_._1).distinct((t) => t).collect()
      val rightIds = right.map(_._1).distinct((t) => t).collect()

      val leftOut = left.flatMap((l, out: Collector[FlinkMetaType]) => {
        rightIds.map((r) => {
          out.collect((Hashing.md5().hashString((l._1.toString + r.toString).toString, Charsets.UTF_8).asLong(), l._2, l._3))
        })
      })

      val rightOut = right.flatMap((r, out: Collector[FlinkMetaType]) => {
        leftIds.map((l) => {
          out.collect((Hashing.md5().hashString((l.toString + r._1.toString).toString, Charsets.UTF_8).asLong(), r._2, r._3))
        })
      })

      leftOut.union(rightOut)
    }
  }

  def extractPairs(groups : DataSet[FlinkMetaJoinType2]): DataSet[(Long, Long)] = {
    val pair : DataSet[(Long, Long)] = groups.flatMap((g, out:Collector[(Long, Long)]) => {
      g._2.map((id) => {
        out.collect((g._1, id))
      })
    })

    pair.join(pair).where(1).equalTo(1){
      (l,r, out : Collector[(Long, Long)]) => {
        if(l._1 != r._1){
          out.collect((r._1, l._1))
        }
      }
    }.distinct
  }

    /*
    val left = executor.implement_md(leftDataset, env).collect()
    val right = executor.implement_md(rightDataset, env).collect()

    val leftId = left.map(_._1).distinct
    val rightIds = right.map(_._1).distinct

    //PURE SCALA CODE except 1 line
    val res =
      if (grouping.isDefined) {
        //EXCEPTION FLINK COLLECT!
        val groups = executor.implement_mjd(grouping.get, env).collect
        assignGroups(left, groups)
          .flatMap((l) => {
          assignGroups(right, groups).flatMap((r) => {
            if (l._1.equals(r._1)) {
              List(
                (Hashing.md5().hashString((l._2.toString + r._2.toString).toString, Charsets.UTF_8).asLong(), l._3, l._4),
                (Hashing.md5().hashString((l._2.toString + r._2.toString).toString, Charsets.UTF_8).asLong(), r._3, r._4)
              )
            } else {
              None
            }
          })
        })
      } else {
        left.flatMap((l) => {
          rightIds.flatMap((r) => {
            Some(
              (Hashing.md5().hashString((l._1.toString + r.toString).toString, Charsets.UTF_8).asLong(), l._2, l._3)
            )
          })
        })
        left.flatMap((l) => {
          rightIds.flatMap((r) => {
            Some(
              (Hashing.md5().hashString((l._1.toString + r.toString).toString, Charsets.UTF_8).asLong(), l._2, l._3)
            )
          })
        })
      }
    //END SCALA CODE
    env.fromCollection(res)

  }

  //SCALA CODE
  def assignGroups(ds: /*mutable.Buffer TODO*/Seq[(Long, String, String)], groups: /*mutable.Buffer TODO*/Seq[(Long, List[Long])]): /*mutable.BufferTODO*/ Seq[(String, Long, String, String)] = {
    val grouped =
      ds.flatMap((d) => {
        groups.flatMap((g) => {
          if(d._1.equals(g._1)){
            g._2.flatMap((gId) => {
              Some((gId.toString, d._1, d._2, d._3))
            })
          } else {
            None
          }
        })
      })
    //logger.error("-------------------- grouped " + grouped)
    grouped
  }

*/

    /*
      val left = executor.implement_md(leftDataset, env)
      val right = executor.implement_md(rightDataset, env)

      if(!grouping.isDefined){
        left.join(right).where(1).equalTo(1){
          (l : FlinkMetaType, r : FlinkMetaType) => {
            (hf.newHasher.putString(l._1.toString + r._1.toString, Charsets.UTF_8).hash.asLong, l._2, /*TODO aggregation?*/l._3)
          }
        }
      } else {
        val groups: DataSet[FlinkMetaJoinType] = executor.implement_mjd(grouping.get, env)
        assignGroups(left, groups).join(assignGroups(right, groups)).where(0).equalTo(0){
          (l : (String,Long,String,String), r : (String,Long,String,String)) => {
            (hf.newHasher.putString(l._2.toString + r._2.toString, Charsets.UTF_8).hash.asLong, l._3, /*TODO aggregation?*/l._4)
          }
        }
      }
    }

  def assignGroups(ds : DataSet[FlinkMetaType], groups : DataSet[FlinkMetaJoinType]) : DataSet[(String,Long,String,String)]= {
    ds.join(groups).where(0).equalTo(0){
      (m : FlinkMetaType, l : (FlinkMetaJoinType), out: Collector[(String,Long,String,String)]) => {
        l._2.map((id : Long) => {
          out.collect((id.toString + "§" + m._2, m._1, m._2, m._3))
        })
      }
    }
  }

  */

}
