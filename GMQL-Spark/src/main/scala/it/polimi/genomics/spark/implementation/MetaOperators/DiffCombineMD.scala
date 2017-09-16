package it.polimi.genomics.spark.implementation.MetaOperators

import com.google.common.hash.Hashing
import it.polimi.genomics.core.DataStructures.{SomeMetaJoinOperator, OptionalMetaJoinOperator, MetaJoinOperator, MetaOperator}
import it.polimi.genomics.core.DataTypes.MetaType
import it.polimi.genomics.core.exception.SelectFormatException
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
 * Created by Abdulrahman Kaitoua on 03/05/17.
 */
object DiffCombineMD{

  //val hf : HashFunction = Hashing.sha256()

  //final val logger = LoggerFactory.getLogger(this.getClass)
  private final val logger = LoggerFactory.getLogger(CombineMD.getClass);

  @throws[SelectFormatException]
  def apply(executor : GMQLSparkExecutor, grouping : OptionalMetaJoinOperator, leftDataset : MetaOperator, rightDataset : MetaOperator, leftTag:String = "left", rightTag:String = "right", sc : SparkContext) : RDD[MetaType] = {

    logger.info("----------------CombineMD executing..")

    val left = executor.implement_md(leftDataset, sc)
    val right = executor.implement_md(rightDataset, sc)

    val ltag = if (!leftTag.isEmpty()){leftTag +"." } else ""
    val rtag = if (!rightTag.isEmpty()){rightTag +"." } else ""

    if (grouping.isInstanceOf[SomeMetaJoinOperator]) {
      val pairs = executor.implement_mjd(grouping, sc).collectAsMap()


      val leftOut = left.flatMap{ l => val pair = pairs.get(l._1)
        if(pair.isDefined){
          /*pair.get.map(expID =>*/ Some(Hashing.md5().newHasher().putLong(l._1)/*.putLong(expID)*/.hash().asLong, (ltag + l._2._1, l._2._2))/*)*/}
        else None
      }

//      val rightOut = right.flatMap{ r =>
//        pairs.flatMap{p =>
//          if (p._2.contains(r._1)) {
//            Some(Hashing.md5().newHasher().putLong(p._1)/*.putLong(r._1)*/.hash().asLong, (rtag + r._2._1, r._2._2))
//          }else None
//        }
//      }

      leftOut//.union(rightOut)

    } else {
      val leftIds = left.keys.distinct().collect()
//      val rightIds = right.keys.distinct().collect()

      val leftOut = left.map{l=>
//        leftIds.map{r =>
          (Hashing.md5().newHasher().putLong(l._1)/*.putLong( r)*/.hash.asLong, (ltag + l._2._1,l._2._2))
//        }
      }

//      val rightOut = right.flatMap{r=>
//        leftIds.map{l =>
//          (Hashing.md5().newHasher().putLong(l)/*.putLong( r._1)*/.hash.asLong, (rtag + r._2._1,r._2._2))
//        }
//      }

      leftOut//.union(rightOut)//.sortBy(x=>x._1)
    }
  }
}
