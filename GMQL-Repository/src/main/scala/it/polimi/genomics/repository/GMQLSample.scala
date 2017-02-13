package it.polimi.genomics.repository

import it.polimi.genomics.core.DataStructures.IRDataSet

/**
  * Created by Abdulrahman Kaitoua on 11/04/16.
  */



/**
  *
  * @param dataSet
  * @param numSamples
  * @param samplesSizeInMB
  * @param totalSizeInMB
  */
case class GMQLStatistics(dataSet:IRDataSet, numSamples:Int, samplesSizeInMB:List[(GMQLSample,Float)], totalSizeInMB:Float)

/**
  *
  * @param name
  * @param ID
  */
case class GMQLSample(name:String="nothing", meta:String ="nothing.meta", ID:String=null)