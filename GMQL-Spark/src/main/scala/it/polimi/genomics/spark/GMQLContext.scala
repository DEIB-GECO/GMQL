package it.polimi.genomics.spark

import org.apache.spark.SparkContext

/**
 * Created by Abdulrahman Kaitoua on 29/05/15.
 * Email: abdulrahman.kaitoua@polimi.it
 *
 */
class GMQLContext (val sc:SparkContext,var metaParallel: Int,val regionParallel:Int){

}
