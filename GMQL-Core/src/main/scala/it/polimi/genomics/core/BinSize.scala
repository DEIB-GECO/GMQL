package it.polimi.genomics.core

/**
  * Created by abdulrahman on 23/01/2017.
  */
/**
  *  Binning parameters for Map, Join, Cover operations.
  *  They are differnt since the implementation acts differently based on the bin size.
  *
  * @param Map [[ Long} of the bin size for the Genometric Map Operation
  * @param Join [[ Long} of the bin size for the Genometric Join Operation
  * @param Cover [[ Long} of the bin size for the Genometric Cover Operation
  */
case class BinSize(Map:Long = 50000l,Join:Long = 500000l,Cover:Long = 2000l)
