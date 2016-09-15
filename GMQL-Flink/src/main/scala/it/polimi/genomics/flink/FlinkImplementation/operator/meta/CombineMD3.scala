package it.polimi.genomics.flink.FlinkImplementation.operator.meta

import it.polimi.genomics.core.DataStructures.{SomeMetaJoinOperator, OptionalMetaJoinOperator, MetaJoinOperator, MetaOperator}
import it.polimi.genomics.core.DataTypes.{FlinkMetaJoinType, FlinkMetaType}
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import org.apache.flink.api.scala._
import org.apache.flink.hadoop.shaded.com.google.common.hash.Hashing
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
 * Created by michelebertoni on 13/05/15.
 */
object  CombineMD3{


  //val hf : HashFunction = Hashing.sha256()
  final val logger = LoggerFactory.getLogger(this.getClass)

  @throws[SelectFormatException]
  def apply(executor : FlinkImplementation, grouping : OptionalMetaJoinOperator, leftDataset : MetaOperator, rightDataset : MetaOperator, leftNameIn : String = "left", rightNameIn : String = "right", env : ExecutionEnvironment) : DataSet[FlinkMetaType] = {

    //logger.warn("Executing CombineMD3")

    val leftName = leftNameIn + "."
    val rightName = rightNameIn + "."

    val left = executor.implement_md(leftDataset, env).map((meta) => (meta._1, leftName  + meta._2, meta._3))
    val right = executor.implement_md(rightDataset, env).map((meta) => (meta._1, rightName  + meta._2, meta._3))

    val pairs: DataSet[(Long, Long)] =
      /*if(grouping.isInstanceOf[SomeMetaJoinOperator])*/{
        val groups : DataSet[FlinkMetaJoinType] =
          executor.implement_mjd3(grouping, env)

        //groups.print

        groups
          .join(groups).where(1).equalTo(1)
          .filter(x=>x._1._3==x._2._4).map(x=>(x._1._1,x._2._1)).distinct

      }
//    else {
//        left.map(_._1).distinct((t) => t).cross(right.map(_._1).distinct((t) => t))
//      }
    //(sampleID, sampleID)

    val leftOut : DataSet[(Long, String, String)] =
      left.join(pairs).where(0).equalTo(0) {
        (m: (Long, String, String), p: (Long, Long), out: Collector[FlinkMetaType]) => {
          val s = new StringBuilder
          s.append(m._1.toString)
          s.append(p._2.toString)
          //val id = Hashing.md5().hashString(s.toString, Charsets.UTF_8).asLong()
          val id : Long = Hashing.md5().newHasher().putLong(m._1).putLong(p._2).hash().asLong()
          out.collect(id, m._2, m._3)
        }
      }

    val rightOut : DataSet[(Long, String, String)] =
      right.join(pairs).where(0).equalTo(1) {
        (m: (Long, String, String), p: (Long, Long), out: Collector[FlinkMetaType]) => {
          val s = new StringBuilder
          s.append(p._1.toString)
          s.append(m._1.toString)
          //val id = Hashing.md5().hashString(s.toString, Charsets.UTF_8).asLong()
          val id : Long = Hashing.md5().newHasher().putLong(p._1).putLong(m._1).hash().asLong()
          out.collect(id, m._2, m._3)
        }
      }

    leftOut.union(rightOut)
  }

/*
    if (grouping.isDefined) {
      val pairs : Seq[(Long, Long)] =
        executor.implement_mjd(grouping.get, env).collect()

      val leftOut : DataSet[(Long, String, String)] =
        left.flatMap((l, out: Collector[FlinkMetaType]) => {
          pairs.map((p) => {
            if(l._1.equals(p._1)) {
              out.collect((Hashing.md5().hashString((l._1.toString + p._2.toString).toString, Charsets.UTF_8).asLong(), l._2, l._3))
            }
          })
        })

      val rightOut : DataSet[(Long, String, String)] =
        right.flatMap((r, out: Collector[FlinkMetaType]) => {
          pairs.map((p) => {
            if(r._1.equals(p._1)) {
              out.collect((Hashing.md5().hashString((p._2.toString + r._1.toString).toString, Charsets.UTF_8).asLong(), r._2, r._3))
            }
          })
        })

      leftOut.union(rightOut)


    } else {
      val leftIds : Seq[Long] =
        left.map(_._1).distinct((t) => t).collect()
      val rightIds : Seq[Long] =
        right.map(_._1).distinct((t) => t).collect()

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
  */

  /*
  def extractPairs(groups : DataSet[FlinkMetaJoinType2]): DataSet[(Long, Long)] = {
    val pair : DataSet[(Long, Long)] =
      groups
        .flatMap((g, out:Collector[(Long, Long)]) => {
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
*/
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



  def assignGroups(ds : DataSet[FlinkMetaType], groups : DataSet[FlinkMetaJoinType]) : DataSet[(Long,String,String, Long)] = {
    ds.join(groups).where(0).equalTo(0){
      (m : FlinkMetaType, l : (FlinkMetaJoinType), out: Collector[(Long,String,String, Long)]) => {
        out.collect((m._1, m._2, m._3, l._2))
      }
    }
  }

  */
}
