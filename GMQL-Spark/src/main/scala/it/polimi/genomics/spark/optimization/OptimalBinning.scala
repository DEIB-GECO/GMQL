package it.polimi.genomics.spark.optimization

import it.polimi.genomics.core.BinSize
import it.polimi.genomics.core.DataStructures.GMQLDatasetProfile
import it.polimi.genomics.core.DataStructures.JoinParametersRD.JoinQuadruple

object OptimalBinning {

  def getJoinOptimalSize( anchor: GMQLDatasetProfile, experiment: GMQLDatasetProfile, distanceJoinCondition : List[JoinQuadruple]) : Long = {

    new BinSize().Join
  }

  def getMapOptimalSize( reference: GMQLDatasetProfile, experiment: GMQLDatasetProfile) : Long = {
    new BinSize().Map
  }


}
