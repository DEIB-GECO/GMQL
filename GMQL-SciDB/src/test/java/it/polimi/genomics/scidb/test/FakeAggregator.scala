package it.polimi.genomics.scidb.test

import it.polimi.genomics.core.DataStructures.RegionAggregate.{RegionsToMeta, RegionsToRegion}
import it.polimi.genomics.core.GValue
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.core.ParsingType._

/**
  * Created by Cattani Simone on 11/04/16.
  * Email: simone.cattani@mail.polimi.it
  *
  */
class FakeR2R extends RegionsToRegion {
  override val resType: PARSING_TYPE = INTEGER
  override val funOut: (GValue, (Int, Int)) => GValue = null
  override val index: Int = -1
  override val fun: (List[GValue]) => GValue = null
  override val associative: Boolean = true
}

class FakeR2M extends RegionsToMeta {
  override val newAttributeName: String = ""
  override val inputIndex: Int = 0
  override val funOut: (GValue, (Int, Int)) => GValue = null
  override val fun: (List[GValue]) => GValue = null
  override val associative: Boolean = true
}
