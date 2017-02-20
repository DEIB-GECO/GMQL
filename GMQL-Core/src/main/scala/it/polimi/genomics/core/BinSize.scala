package it.polimi.genomics.core

/**
  * Created by abdulrahman on 23/01/2017.
  */
/**
  *  Binning parameters for Map, Join, Cover operations.
  *  They are differnt since the implementation acts differently based on the bin size.
  *
  * @param Map {@link Long} of the bin size for the Genometric Map Operation
  * @param Join {@link Long} of the bin size for the Genometric Join Operation
  * @param Cover {@link Long} of the bin size for the Genometric Cover Operation
  */
case class BinSize(Map:Long = 5000l,Join:Long = 5000l,Cover:Long = 1000l)
