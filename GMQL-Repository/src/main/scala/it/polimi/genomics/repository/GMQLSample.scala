package it.polimi.genomics.repository

import it.polimi.genomics.core.DataStructures.IRDataSet

/**
  * Created by Abdulrahman Kaitoua on 11/04/16.
  */



/**
  *
  *  Set of profiling information needed to be collected for every dataset.
  *
  * @param dataSet
  * @param numSamples
  * @param samplesSizeInMB
  * @param totalSizeInMB
  */
case class GMQLStatistics(dataSet:IRDataSet, numSamples:Int, samplesSizeInMB:List[(GMQLSample,Float)], totalSizeInMB:Float)

/**
  *  GMQLSample is an abstraction  of the path of the sample and its metadata along with the ID
  * @param name Path or name of the sample
  * @param meta Path or name of the sample meta data
  * @param ID Integer of the id of the sample
  */
case class GMQLSample(name:String="nothing", meta:String ="nothing.meta", ID:String=null)