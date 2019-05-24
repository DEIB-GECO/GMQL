package it.polimi.genomics.core.DataStructures

import it.polimi.genomics.core.DAG.DependencyException
import it.polimi.genomics.core.DataStructures.CoverParameters.CoverFlag.CoverFlag
import it.polimi.genomics.core.DataStructures.CoverParameters.CoverParam
import it.polimi.genomics.core.DataStructures.GroupMDParameters.Direction.Direction
import it.polimi.genomics.core.DataStructures.GroupMDParameters.TopParameter
import it.polimi.genomics.core.DataStructures.JoinParametersRD.JoinQuadruple
import it.polimi.genomics.core.DataStructures.JoinParametersRD.RegionBuilder.RegionBuilder
import it.polimi.genomics.core.DataStructures.MetaAggregate.{MetaAggregateFunction, MetaExtension}
import it.polimi.genomics.core.DataStructures.MetaGroupByCondition.MetaGroupByCondition
import it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition
import it.polimi.genomics.core.DataStructures.MetadataCondition.MetadataCondition
import it.polimi.genomics.core.DataStructures.RegionAggregate.{RegionFunction, RegionsToMeta, RegionsToRegion}
import it.polimi.genomics.core.DataStructures.RegionCondition.RegionCondition
import it.polimi.genomics.core.GMQLLoader
import it.polimi.genomics.core.ParsingType.PARSING_TYPE


case class IRNoopMD() extends MetaOperator {
  /** Returns the list of dependencies of the node */
  override def getDependencies: List[IROperator] = List()

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator =
    throw new DependencyException("This operator has no dependencies!")
}

case class IRNoopRD() extends RegionOperator {
  /** Returns the list of dependencies of the node */
  override def getDependencies: List[IROperator] = List()

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator =
    throw new DependencyException("This operator has no dependencies!")
}


/**
  * Dag node to represent the repository/storage reader for metadata files.
  *
  * @param paths  path from which to read the files
  * @param loader class that contains both the region and the metadata parser
  * @tparam IR Type of the input of the region parser
  * @tparam OR Type of the output of the region parser
  * @tparam IM Type of the input of the metadata parser
  * @tparam OM Type of the output of the metadata parser
  */
case class IRReadMD[IR, OR, IM, OM](var paths: List[String],
                                    loader: GMQLLoader[IR, OR, IM, OM],
                                    var dataset: IRDataSet) extends MetaOperator with ReadOperator {
  override def getDependencies: List[IROperator] = List()

  override def sources: Set[IRDataSet] = Set(dataset)

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator =
    throw new DependencyException("This operator has no dependencies!")
}

/**
  * Dag node to represent the memory dataset reader for meta data.
  */
case class IRReadMEMMD(var metaDS: Any) extends MetaOperator {
  override def getDependencies: List[IROperator] = List()

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator =
    throw new DependencyException("This operator has no dependencies!")
}


/**
  * Dag node to represent the repository/storage reader for region data files.
  *
  * @param paths  path from which to read the files
  * @param loader class that contains both the region and the metadata parser
  * @tparam IR Type of the input of the region parser
  * @tparam OR Type of the output of the region parser
  * @tparam IM Type of the input of the metadata parser
  * @tparam OM Type of the output of the metadata parser
  */
case class IRReadRD[IR, OR, IM, OM](var paths: List[String],
                                    loader: GMQLLoader[IR, OR, IM, OM],
                                    var dataset: IRDataSet) extends RegionOperator with ReadOperator {
  override def getDependencies: List[IROperator] = List()

  override def sources: Set[IRDataSet] = Set(dataset)

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator =
    throw new DependencyException("This operator has no dependencies!")
}

/**
  * Dag node to represent the memory dataset reader for region data.
  */
case class IRReadMEMRD(regionDS: Any) extends RegionOperator {
  override def getDependencies: List[IROperator] = List()

  //override def substituteDependency(previousDependency: IROperator, newDependency: IROperator): IROperator = this.copy()
  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator =
    throw new DependencyException("This operator has no dependencies!")
}


case class IRReadFedRD(name: String, var path:Option[String] = None) extends RegionOperator with Federated {
  /** Returns the list of dependencies of the node */
  override def getDependencies: List[IROperator] = Nil

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator =
    throw new DependencyException("This operator has no dependencies!")

  override def toString: String = super.toString + "\n" + this.name
}


case class IRReadFedMD(name: String, var path:Option[String] = None) extends MetaOperator with Federated {
  /** Returns the list of dependencies of the node */
  override def getDependencies: List[IROperator] = Nil

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator =
    throw new DependencyException("This operator has no dependencies!")

  override def toString: String = super.toString + "\n" + this.name
}


case class IRReadFedMetaJoin(name: String, var path:Option[String] = None) extends MetaJoinOperator with Federated {
  /** Returns the list of dependencies of the node */
  override def getDependencies: List[IROperator] = Nil

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator =
    throw new DependencyException("This operator has no dependencies!")

  override def toString: String = super.toString + "\n" + this.name
}


case class IRReadFedMetaGroup(name: String, var path:Option[String] = None) extends MetaGroupOperator with Federated {
  /** Returns the list of dependencies of the node */
  override def getDependencies: List[IROperator] = Nil

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator =
    throw new DependencyException("This operator has no dependencies!")

  override def toString: String = super.toString + "\n" + this.name
}


case class IRStoreFedRD(input: RegionOperator, name: String, var path:Option[String] = None) extends RegionOperator with Federated {
  /** Returns the list of dependencies of the node */
  override def getDependencies: List[IROperator] = List(input)

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator = {
    if (oldDep == input && newDep.isRegionOperator) this.copy(input = newDep.asInstanceOf[RegionOperator])
    else throw new DependencyException
  }

  override def toString: String = super.toString + "\n" + this.name
}

case class IRStoreFedMD(input: MetaOperator, name: String, var path:Option[String] = None) extends MetaOperator with Federated {
  /** Returns the list of dependencies of the node */
  override def getDependencies: List[IROperator] = List(input)

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator =
    if (oldDep == input && newDep.isMetaOperator) this.copy(input = newDep.asInstanceOf[MetaOperator])
    else throw new DependencyException

  override def toString: String = super.toString + "\n" + this.name
}

case class IRStoreFedMetaJoin(input: MetaJoinOperator, name: String, var path:Option[String] = None) extends MetaJoinOperator with Federated {
  /** Returns the list of dependencies of the node */
  override def getDependencies: List[IROperator] = List(input)

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator =
    if (oldDep == input && newDep.isMetaJoinOperator) this.copy(input = newDep.asInstanceOf[MetaJoinOperator])
    else throw new DependencyException

  override def toString: String = super.toString + "\n" + this.name
}

case class IRStoreFedMetaGroup(input: MetaGroupOperator, name: String, var path:Option[String] = None) extends MetaGroupOperator with Federated {
  /** Returns the list of dependencies of the node */
  override def getDependencies: List[IROperator] = List(input)

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator =
    if (oldDep == input && newDep.isMetaGroupOperator) this.copy(input = newDep.asInstanceOf[MetaGroupOperator])
    else throw new DependencyException

  override def toString: String = super.toString + "\n" + this.name
}

//TODO Nanni
///**
//  * Dag node to load single files with custom loader
//  * @param path path of the file
//  * @param loader class that contains both the region and the metadata parser
//  * @param dataset dataset class which is a wrapper for the dataset
//  * @tparam IR Type of the input of the region parser
//  * @tparam OR Type of the output of the region parser
//  * @tparam IM Type of the input of the metadata parser
//  * @tparam OM Type of the output of the metadata parser
//  */
//case class IRReadFileRD[IR,OR,IM,OM](var path: String,
//                                   loader: GMQLLoader[IR,OR,IM,OM],
//                                   var dataset : IRDataSet) extends RegionOperator
//
//
///**
//  * Dag node to generate empty metadata values for each key in the region data
//  * @param regionFile region Dag node
//  */
//case class IRGenerateMD(regionFile: RegionOperator) extends MetaOperator


/** Dag node to represent the MATERIALIZE operation of a variable's metadata into the repository/storage.
  *
  * @param path   location where to store the metadata
  * @param father metadata dag of the variable to be stored
  */
case class IRStoreMD(var path: String, var father: MetaOperator, var dataSet: IRDataSet) extends MetaOperator {
  override def getDependencies: List[IROperator] = List(this.father)

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator = {
    if (oldDep == father && newDep.isMetaOperator)
      this.copy(father = newDep.asInstanceOf[MetaOperator])
    else throw new DependencyException
  }
}


/** Dag node to represent the MATERIALIZE operation of a variable's regions into the repository/storage.
  *
  * @param path   location where to store the regions
  * @param father metadata dag of the variable to be stored
  */
case class IRStoreRD(var path: String, var father: RegionOperator, var associatedMeta: MetaOperator,
                     schema: List[(String, PARSING_TYPE)], dataSet: IRDataSet) extends RegionOperator {
  override def getDependencies: List[IROperator] = List(father, associatedMeta)

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator = {
    if (oldDep == father && newDep.isRegionOperator)
      this.copy(father = newDep.asInstanceOf[RegionOperator])
    else if (oldDep == associatedMeta && newDep.isMetaOperator)
      this.copy(associatedMeta = newDep.asInstanceOf[MetaOperator])
    else throw new DependencyException
  }
}

/**
  * Dag node to represent the matadata filtering performed by the SELECT operator.
  *
  * @param meta_cond     the condition to be applied
  * @param input_dataset the input metadata set
  */
//TODO MissingAttribute
case class IRSelectMD(meta_cond: MetadataCondition, var input_dataset: MetaOperator) extends MetaOperator {
  override def getDependencies: List[IROperator] = List(input_dataset)

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator = {
    if (oldDep == input_dataset && newDep.isMetaOperator)
      this.copy(input_dataset = newDep.asInstanceOf[MetaOperator])
    else throw new DependencyException
  }
}

/**
  * Dag node to represent the purging of empty dataset to be performed at the end of the SELECT operator
  *
  * @param region_dataset the set of filtered regions
  * @param input_dataset  the set of metadata to be purged
  */
case class IRPurgeMD(var region_dataset: RegionOperator, var input_dataset: MetaOperator) extends MetaOperator {
  override def getDependencies: List[IROperator] = List(region_dataset, input_dataset)

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator = {
    if (oldDep == region_dataset && newDep.isRegionOperator)
      this.copy(region_dataset = newDep.asInstanceOf[RegionOperator])
    else if (oldDep == input_dataset && newDep.isMetaOperator)
      this.copy(input_dataset = newDep.asInstanceOf[MetaOperator])
    else throw new DependencyException
  }
}

/**
  * Dag node to represent the purging of a region dataset.
  * Only the tuples in the second dataset which ID appears in the first dataset must be kept.
  *
  * @param meta_dataset  metadata dataset used for filtering
  * @param input_dataset input region dataset to be filtered
  */
case class IRPurgeRD(var meta_dataset: MetaOperator, var input_dataset: RegionOperator) extends RegionOperator {
  override def getDependencies: List[IROperator] = List(meta_dataset, input_dataset)

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator = {
    if (oldDep == input_dataset && newDep.isRegionOperator)
      this.copy(input_dataset = newDep.asInstanceOf[RegionOperator])
    else if (oldDep == meta_dataset && newDep.isMetaOperator)
      this.copy(meta_dataset = newDep.asInstanceOf[MetaOperator])
    else throw new DependencyException
  }
}


/**
  * Dag node to represent the region filtering performed by the SELECT operator.
  *
  * @param reg_cond      the condition to be applied
  * @param filtered_meta the filtered metadata for this set; it is None if the SELECT operator does not have a condition on the metadata or a semijoin
  * @param input_dataset the input region set
  */
case class IRSelectRD(reg_cond: Option[RegionCondition], var filtered_meta: Option[MetaOperator],
                      var input_dataset: RegionOperator) extends RegionOperator {
  override def getDependencies: List[IROperator] = {
    if (filtered_meta.isDefined) List(filtered_meta.get, input_dataset) else List(input_dataset)
  }

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator = {
    if (oldDep == input_dataset && newDep.isRegionOperator)
      this.copy(input_dataset = newDep.asInstanceOf[RegionOperator])
    else if (filtered_meta.isDefined) {
      if (oldDep == filtered_meta.get && newDep.isMetaOperator)
        this.copy(filtered_meta = Some(newDep.asInstanceOf[MetaOperator]))
      else throw new DependencyException
    }
    else throw new DependencyException
  }
}

/**
  * Dag node to represent the semi-join condition in the SELECT operator
  *
  * @param external_meta  The external dataset that is read for comparison
  * @param join_condition The condition to check
  * @param input_dataset  The meta data of the variable that is going to be selected
  */
case class IRSemiJoin(var external_meta: MetaOperator, join_condition: MetaJoinCondition,
                      var input_dataset: MetaOperator) extends MetaOperator {
  override def getDependencies: List[IROperator] = List(external_meta, input_dataset)

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator = {
    var res = this.copy()
    var hasMatch = false
    if (oldDep == external_meta && newDep.isMetaOperator) {
      res = res.copy(external_meta = newDep.asInstanceOf[MetaOperator])
      hasMatch = true
    }

    if (oldDep == input_dataset && newDep.isMetaOperator) {
      res = res.copy(input_dataset = newDep.asInstanceOf[MetaOperator])
      hasMatch = true
    }

    if (hasMatch) res
    else throw new DependencyException
  }
}

/**
  * Dag node to represent the projection of a metadata set.
  * The project can keep only certain metadata tuple or produce new ones or both.
  * The result must be the set of all the metadata tuple which attribute is in the [[projected_attributes]] plus
  * all the new tuple produced by the functions in [[extended_attributes]]
  *
  * @param projected_attributes list of the attributes to be kept
  * @param extended_attributes  list of the new attributes to be produced
  * @param all_but_flag         set to true in case of negating the projected attributes. false is the default.
  * @param input_dataset        input set of metadata
  */
//TODO cambia: togliere distinct e extended
case class IRProjectMD(projected_attributes: Option[List[String]],
                       extended_attributes: Option[List[MetaExtension]],
                       all_but_flag: Boolean,
                       var input_dataset: MetaOperator) extends MetaOperator {
  override def getDependencies: List[IROperator] = List(input_dataset)

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator = {
    if (oldDep == input_dataset && newDep.isMetaOperator)
      this.copy(input_dataset = newDep.asInstanceOf[MetaOperator])
    else throw new DependencyException
  }
}

/**
  * Dag node that represent the projection, distinct and extension of a set of region tuples.
  *
  * @param projected_values if not [[None]], it stores the position of the values that have to be kept
  * @param new_values       if not [[None]], it stores all the function to create new value fields
  * @param input_dataset    the input set of region data
  */
case class IRProjectRD(projected_values: Option[List[Int]],
                       new_values: Option[List[RegionFunction]],
                       var input_dataset: RegionOperator,
                       var InputMeta: MetaOperator) extends RegionOperator {
  override def getDependencies: List[IROperator] = List(input_dataset, InputMeta)

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator = {
    if (oldDep == input_dataset && newDep.isRegionOperator)
      this.copy(input_dataset = newDep.asInstanceOf[RegionOperator])
    else if (oldDep == InputMeta && newDep.isMetaOperator)
      this.copy(InputMeta = newDep.asInstanceOf[MetaOperator])
    else throw new DependencyException
  }
}

/**
  * It takes as input a dataset of regions and a list of aggregations function. It aggregates the regions by ID and apply to each group all the funcion in the list.
  * As result produces a dataset of new metadata tuples
  *
  * @param aggregates    list of aggregation function, along with the name of the new produced attribute
  * @param input_dataset the input region set
  */
case class IRAggregateRD(aggregates: List[RegionsToMeta], var input_dataset: RegionOperator) extends MetaOperator {
  override def getDependencies: List[IROperator] = List(input_dataset)

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator = {
    if (oldDep == input_dataset && newDep.isRegionOperator)
      this.copy(input_dataset = newDep.asInstanceOf[RegionOperator])
    else throw new DependencyException
  }
}


/**
  * Partition the dataset into groups and creates a new metadata for each sample indicating the belonging group.
  * Calculates aggregate function for each group separately.
  *
  * @param keys           the keys to be used for creating the groups
  * @param aggregates     the list of [[MetaAggregateFunction]] to be applied to each group
  * @param input_dataset  the dataset to be grouped
  * @param group_name     it indicates the name for the new grouping attribute
  * @param region_dataset region dataset on which aggregate functions will be evaluated
  */
case class IRGroupMD(keys: MetaGroupByCondition,
                     aggregates: Option[List[MetaAggregateFunction]],
                     group_name: String,
                     var input_dataset: MetaOperator,
                     var region_dataset: RegionOperator) extends MetaOperator {
  override def getDependencies: List[IROperator] = List(input_dataset, region_dataset)

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator = {
    if (oldDep == input_dataset && newDep.isMetaOperator)
      this.copy(input_dataset = newDep.asInstanceOf[MetaOperator])
    else if (oldDep == region_dataset && newDep.isRegionOperator)
      this.copy(region_dataset = newDep.asInstanceOf[RegionOperator])
    else throw new DependencyException
  }
}

/**
  * Partition the regions of each dataset in disjunctive sets. Only one region for each partion is returned as output.
  * The grouping is made on the key values, those values can include zero,one or more of the region's attributes.
  * The region ID and its coordinates are always implicitly used for the grouping.
  * Then a set of aggregate function is computed on each group.
  * Each output region only has: the coordinates, all the region's values which have been used for grouping and the new computed values.
  * All the other input region's values are discarded.
  *
  * @param grouping_parameters The parameter to group regions, if not specified (None), then only chr,start, stop and strand are used
  * @param aggregates          The list of aggregate function to apply to each set
  * @param region_dataset      The input region set
  */
case class IRGroupRD(grouping_parameters: Option[List[GroupRDParameters.GroupingParameter]],
                     aggregates: Option[List[RegionAggregate.RegionsToRegion]],
                     var region_dataset: RegionOperator) extends RegionOperator {
  override def getDependencies: List[IROperator] = List(region_dataset)

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator = {
    if (oldDep == region_dataset && newDep.isRegionOperator)
      this.copy(region_dataset = newDep.asInstanceOf[RegionOperator])
    else throw new DependencyException
  }
}

/**
  * Order the samples according to the value of a metadata attribute. If TOP(k) is present, only the first k samples in the order
  * are kept. If TopG(k) is present, only the first k element of every group are kept.
  *
  * @param ordering      couples (attribute_name, direction)
  * @param new_attribute attribute name to be inserted to specify the position of the sample
  * @param top_par       parameters for the top
  * @param input_dataset metadata set to be ordered
  */
case class IROrderMD(ordering: List[(String, Direction)], new_attribute: String, top_par: TopParameter,
                     var input_dataset: MetaOperator) extends MetaOperator {
  override def getDependencies: List[IROperator] = List(input_dataset)

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator = {
    if (oldDep == input_dataset && newDep.isMetaOperator)
      this.copy(input_dataset = newDep.asInstanceOf[MetaOperator])
    else throw new DependencyException
  }
}


/**
  * Order the regions in each sample of a given dataset.The ordering is done on the value of one or more region's values.
  * Optionally, returns only the first TOP K or TOPG K regions,
  * For each region, a new value is appended, storing the position in the ranking
  *
  * @param ordering      The ordering condition, made of couples (value_position, ordering_direction)
  * @param top_par       The number of regions to be in the output
  * @param input_dataset The region set to order
  */

case class IROrderRD(ordering: List[(Int, Direction)], top_par: TopParameter,
                     var input_dataset: RegionOperator) extends RegionOperator {
  override def getDependencies: List[IROperator] = List(input_dataset)

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator = {
    if (oldDep == input_dataset && newDep.isRegionOperator)
      this.copy(input_dataset = newDep.asInstanceOf[RegionOperator])
    else throw new DependencyException
  }
}

/**
  * The metadata grouping
  *
  * @param group_attributes condition that specifies the attribute that have to be used for the grouping
  * @param input_dataset    the input set of metadata to be grouped
  */
case class IRGroupBy(group_attributes: MetaGroupByCondition,
                     var input_dataset: MetaOperator) extends MetaGroupOperator {
  override def getDependencies: List[IROperator] = List(input_dataset)

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator = {
    if (oldDep == input_dataset && newDep.isMetaOperator)
      this.copy(input_dataset = newDep.asInstanceOf[MetaOperator])
    else throw new DependencyException
  }
}

/**
  * Apply any of the available region intersection operators: COVER, SUMMIT or FLAT.
  *
  * @param cover_flag    the type of operation
  * @param min           the minimun level of accumulation
  * @param max           the maximum level of accumulation
  * @param aggregates    the list (eventually empty) of aggregates functions to be applied to each of the result regions
  * @param groups        is not [[None]] it represents the groups of samples
  * @param input_dataset the region dataset to be processed
  */
case class IRRegionCover(cover_flag: CoverFlag,
                         min: CoverParam, max: CoverParam,
                         aggregates: List[RegionsToRegion],
                         groups: Option[MetaGroupOperator],
                         var input_dataset: RegionOperator) extends RegionOperator {
  override def getDependencies: List[IROperator] =
    if (groups.isDefined) List(input_dataset, groups.get) else List(input_dataset)

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator = {
    if (oldDep == input_dataset && newDep.isRegionOperator)
      this.copy(input_dataset = newDep.asInstanceOf[RegionOperator])
    else if (groups.isDefined) {
      if (oldDep == groups.get && newDep.isMetaGroupOperator)
        this.copy(groups = Some(newDep.asInstanceOf[MetaGroupOperator]))
      else throw new DependencyException
    }
    else throw new DependencyException
  }
}

/**
  * Computes the union of the samples in one dataset
  * It may group them according to a metagroup operator
  * It computes aggregation of the values inside each group
  *
  * @param dataset meta dataset to create the union
  * @param groups  eventual <code>MetaGroupOperator</code> operator
  */
case class IRMergeMD(var dataset: MetaOperator,
                     groups: Option[MetaGroupOperator]) extends MetaOperator {
  override def getDependencies: List[IROperator] = if (groups.isDefined) List(dataset, groups.get) else List(dataset)

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator = {
    if (oldDep == dataset && newDep.isMetaOperator)
      this.copy(dataset = newDep.asInstanceOf[MetaOperator])
    else if (groups.isDefined) {
      if (oldDep == groups.get && newDep.isMetaGroupOperator)
        this.copy(groups = Some(newDep.asInstanceOf[MetaGroupOperator]))
      else throw new DependencyException
    }
    else throw new DependencyException
  }
}

/**
  * Computes the union of the samples in one dataset
  * It may group them according to a metagroup operator
  * It computes aggregation of the values inside each group
  *
  * @param dataset region dataset to create the union
  * @param groups  eventual <code>MetaGroupOperator</code> operator
  */
case class IRMergeRD(var dataset: RegionOperator,
                     groups: Option[MetaGroupOperator]) extends RegionOperator {
  override def getDependencies: List[IROperator] =
    if (groups.isDefined) List(dataset, groups.get) else List(dataset)

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator = {
    if (oldDep == dataset && newDep.isRegionOperator)
      this.copy(dataset = newDep.asInstanceOf[RegionOperator])
    else if (groups.isDefined) {
      if (oldDep == groups.get && newDep.isMetaGroupOperator)
        this.copy(groups = Some(newDep.asInstanceOf[MetaGroupOperator]))
      else throw new DependencyException
    }
    else throw new DependencyException
  }
}

/**
  * Take as parameter two sets of metadata and a condition to apply on their cross product. Return as output
  * //the allowed couple (id1,id2), <---- changed
  * the list of groups for each dataset
  * //where id1 comes from the left dataset and id2 from the right one.
  * in the form of (sampleID, List(groupIDS))
  *
  * @param condition     the join condition
  * @param left_dataset  the first dataset
  * @param right_dataset the second dataset
  */
case class IRJoinBy(condition: MetaJoinCondition, var left_dataset: MetaOperator,
                    var right_dataset: MetaOperator) extends MetaJoinOperator {
  override def getDependencies: List[IROperator] = List(left_dataset, right_dataset)

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator = {
    var res = this.copy()
    var hasMatch = false
    if (oldDep == left_dataset && newDep.isMetaOperator) {
      res = res.copy(left_dataset = newDep.asInstanceOf[MetaOperator])
      hasMatch = true
    }

    if (oldDep == right_dataset && newDep.isMetaOperator) {
      res = res.copy(right_dataset = newDep.asInstanceOf[MetaOperator])
      hasMatch = true
    }

    if (hasMatch) res
    else throw new DependencyException
  }
}

/**
  * Take as parameters two metadata datasets and (optionally) the result of a [[IRJoinBy]], produce as output the union of the allowed metadata in the two datasets.
  *
  * @param grouping      the first metadata set
  * @param right_dataset the second metadata set
  */
case class IRCombineMD(grouping: OptionalMetaJoinOperator, var left_dataset: MetaOperator,
                       var right_dataset: MetaOperator,
                       region_builder: Option[RegionBuilder] = None, left_ds_name: String = "left",
                       right_ds_name: String = "right") extends MetaOperator {
  override def getDependencies: List[IROperator] = List(left_dataset, right_dataset, grouping.getOperator)

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator = {
    var res = this.copy()
    var hasMatch = false
    if (oldDep == left_dataset && newDep.isMetaOperator) {
      res = res.copy(left_dataset = newDep.asInstanceOf[MetaOperator])
      hasMatch = true
    }

    if (oldDep == right_dataset && newDep.isMetaOperator) {
      res = res.copy(right_dataset = newDep.asInstanceOf[MetaOperator])
      hasMatch = true
    }

    if (oldDep == grouping.getOperator && newDep.isMetaJoinOperator) {
      val newGrouping = {
        if (grouping.isInstanceOf[NoMetaJoinOperator]) NoMetaJoinOperator(newDep.asInstanceOf[MetaJoinOperator])
        else if (grouping.isInstanceOf[SomeMetaJoinOperator]) SomeMetaJoinOperator(newDep.asInstanceOf[MetaJoinOperator])
        else throw new IllegalStateException("Grouping parameter is not either NoMetaJoinOperator or SomeMetaJoinOperator")
      }
      res = res.copy(grouping = newGrouping)
      hasMatch = true
    }

    if (hasMatch) res
    else throw new DependencyException
  }
}

/**
  * Take as parameters two metadata datasets and (optionally) the result of a [[IRJoinBy]], produce as output the union of the allowed metadata in the two datasets.
  *
  * @param grouping      the first metadata set
  * @param right_dataset the second metadata set
  */
case class IRDiffCombineMD(grouping: OptionalMetaJoinOperator, var left_dataset: MetaOperator,
                           var right_dataset: MetaOperator,
                           left_ds_name: String = "left", right_ds_name: String = "right") extends MetaOperator {
  override def getDependencies: List[IROperator] = List(left_dataset, right_dataset, grouping.getOperator)

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator = {
    var res = this.copy()
    var hasMatch = false
    if (oldDep == left_dataset && newDep.isMetaOperator) {
      res = res.copy(left_dataset = newDep.asInstanceOf[MetaOperator])
      hasMatch = true
    }

    if (oldDep == right_dataset && newDep.isMetaOperator) {
      res = res.copy(right_dataset = newDep.asInstanceOf[MetaOperator])
      hasMatch = true
    }

    if (oldDep == grouping.getOperator && newDep.isMetaJoinOperator) {
      val newGrouping = {
        if (grouping.isInstanceOf[NoMetaJoinOperator]) NoMetaJoinOperator(newDep.asInstanceOf[MetaJoinOperator])
        else if (grouping.isInstanceOf[SomeMetaJoinOperator]) SomeMetaJoinOperator(newDep.asInstanceOf[MetaJoinOperator])
        else throw new IllegalStateException("Grouping parameter is not either NoMetaJoinOperator or SomeMetaJoinOperator")
      }
      res = res.copy(grouping = newGrouping)
      hasMatch = true
    }

    if (hasMatch) res
    else throw new DependencyException
  }
}

/**
  * Apply the result of a metadata grouping on a metadata set; i.e. it changes the id accordingly to the metadata
  * grouping result
  *
  * @param grouping      the result of a groupby condition
  * @param input_dataset the metadata set
  */
case class IRCollapseMD(grouping: Option[MetaGroupOperator],
                        var input_dataset: MetaOperator) extends MetaOperator {
  override def getDependencies: List[IROperator] =
    if (grouping.isDefined) List(input_dataset, grouping.get) else List(input_dataset)

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator = {
    if (oldDep == input_dataset && newDep.isMetaOperator)
      this.copy(input_dataset = newDep.asInstanceOf[MetaOperator])
    else if (grouping.isDefined) {
      if (oldDep == grouping.get && newDep.isMetaGroupOperator)
        this.copy(grouping = Some(newDep.asInstanceOf[MetaGroupOperator]))
      else throw new DependencyException
    }
    else
      throw new DependencyException
  }
}

/**
  * Compute the difference of two datasets. Returns only the regions in the left_dataset which do not interserc any of the regions in the right_dataset.
  * Optionally, can use a meta-join condition: each region in the left_dataset is checked only agains all the regions in the right_dataset which sample satisfy the
  * meta-join-condition
  *
  * @param meta_join     The optional meta join condition
  * @param left_dataset  The dataset to be cleaned (minuend)
  * @param right_dataset The region that must not be overlapped (subtrahend)
  */
case class IRDifferenceRD(meta_join: OptionalMetaJoinOperator,
                          var left_dataset: RegionOperator,
                          var right_dataset: RegionOperator,
                          exact: Boolean = false) extends RegionOperator {
  override def getDependencies: List[IROperator] = List(left_dataset, right_dataset, meta_join.getOperator)

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator = {
    var res = this.copy()
    var hasMatch = false
    if (oldDep == left_dataset && newDep.isRegionOperator) {
      res = res.copy(left_dataset = newDep.asInstanceOf[RegionOperator])
      hasMatch = true
    }

    if (oldDep == right_dataset && newDep.isRegionOperator) {
      res = res.copy(right_dataset = newDep.asInstanceOf[RegionOperator])
      hasMatch = true
    }

    if (oldDep == meta_join.getOperator && newDep.isMetaJoinOperator) {
      val newGrouping = {
        if (meta_join.isInstanceOf[NoMetaJoinOperator]) NoMetaJoinOperator(newDep.asInstanceOf[MetaJoinOperator])
        else if (meta_join.isInstanceOf[SomeMetaJoinOperator]) SomeMetaJoinOperator(newDep.asInstanceOf[MetaJoinOperator])
        else throw new IllegalStateException("Grouping parameter is not either NoMetaJoinOperator or SomeMetaJoinOperator")
      }
      res = res.copy(meta_join = newGrouping)
      hasMatch = true
    }

    if (hasMatch) res
    else throw new DependencyException
  }
}

/**
  * Represent a generic GenometricJoin operation between two datasets. The left dataset is used as anchor.
  *
  * @param metajoin_condition optional, it is the meta join condition
  * @param join_condition     list of join quadruple; the quadruple are in a disjunctive relationship
  * @param region_builder     the function to be used for computing the new region
  * @param join_on_attributes list of pairs (positionLeft, positionRight), corresponding to the position
  *                           of the join keys in the schema of the left and the right datasets
  * @param left_dataset       left dataset
  * @param right_dataset      right dataset
  */
case class IRGenometricJoin(metajoin_condition: OptionalMetaJoinOperator,
                            join_condition: List[JoinQuadruple],
                            region_builder: RegionBuilder,
                            join_on_attributes: Option[List[(Int, Int)]],
                            var left_dataset: RegionOperator,
                            var right_dataset: RegionOperator) extends RegionOperator {
  override def getDependencies: List[IROperator] = List(left_dataset, right_dataset, metajoin_condition.getOperator)

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator = {
    var res = this.copy()
    var hasMatch = false
    if (oldDep == left_dataset && newDep.isRegionOperator) {
      res = res.copy(left_dataset = newDep.asInstanceOf[RegionOperator])
      hasMatch = true
    }

    if (oldDep == right_dataset && newDep.isRegionOperator) {
      res = res.copy(right_dataset = newDep.asInstanceOf[RegionOperator])
      hasMatch = true
    }

    if (oldDep == metajoin_condition.getOperator && newDep.isMetaJoinOperator) {
      val newGrouping = {
        if (metajoin_condition.isInstanceOf[NoMetaJoinOperator]) NoMetaJoinOperator(newDep.asInstanceOf[MetaJoinOperator])
        else if (metajoin_condition.isInstanceOf[SomeMetaJoinOperator]) SomeMetaJoinOperator(newDep.asInstanceOf[MetaJoinOperator])
        else throw new IllegalStateException("Grouping parameter is not either NoMetaJoinOperator or SomeMetaJoinOperator")
      }
      res = res.copy(metajoin_condition = newGrouping)
      hasMatch = true
    }

    if (hasMatch) res
    else throw new DependencyException
  }
}

/**
  * Apply the MAP operator on a reference and a sample datasets. Optionally, it can take into consideration the result of a meta-join operation and apply a set of aggregate functions
  *
  * @param grouping   If not [[None]], it is the result of the meta-join step.
  * @param aggregates A list (possibly empty) of aggregate function to be applied on every region in the output
  * @param reference  The reference dataset
  * @param samples    The sample dataset
  */
case class IRGenometricMap(grouping: OptionalMetaJoinOperator,
                           aggregates: List[RegionAggregate.RegionsToRegion],
                           var reference: RegionOperator, var samples: RegionOperator) extends RegionOperator {
  override def getDependencies: List[IROperator] = List(reference, samples, grouping.getOperator)

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator = {
    var res = this.copy()
    var hasMatch = false
    if (oldDep == reference && newDep.isRegionOperator) {
      res = res.copy(reference = newDep.asInstanceOf[RegionOperator])
      hasMatch = true
    }

    if (oldDep == samples && newDep.isRegionOperator) {
      res = res.copy(samples = newDep.asInstanceOf[RegionOperator])
      hasMatch = true
    }

    if (oldDep == grouping.getOperator && newDep.isMetaJoinOperator) {
      val newGrouping = {
        if (grouping.isInstanceOf[NoMetaJoinOperator]) NoMetaJoinOperator(newDep.asInstanceOf[MetaJoinOperator])
        else if (grouping.isInstanceOf[SomeMetaJoinOperator]) SomeMetaJoinOperator(newDep.asInstanceOf[MetaJoinOperator])
        else throw new IllegalStateException("Grouping parameter is not either NoMetaJoinOperator or SomeMetaJoinOperator")
      }
      res = res.copy(grouping = newGrouping)
      hasMatch = true
    }

    if (hasMatch) res
    else throw new DependencyException
  }
}

/**
  * Computes the union of the two input datasets.
  * It changes the ID to avoid collision between left and right
  *
  * @param left_dataset  first metadata set
  * @param right_dataset second metadata set
  */
case class IRUnionMD(var left_dataset: MetaOperator, var right_dataset: MetaOperator,
                     left_ds_name: String = "right", right_ds_name: String = "left") extends MetaOperator {
  override def getDependencies: List[IROperator] = List(left_dataset, right_dataset)

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator = {
    var res = this.copy()
    var hasMatch = false
    if (oldDep == left_dataset && newDep.isMetaOperator) {
      res = res.copy(left_dataset = newDep.asInstanceOf[MetaOperator])
      hasMatch = true
    }

    if (oldDep == right_dataset && newDep.isMetaOperator) {
      res = res.copy(right_dataset = newDep.asInstanceOf[MetaOperator])
      hasMatch = true
    }

    if (hasMatch) res
    else throw new DependencyException
  }
}

/**
  * Computes the union of the two input datasets.
  * It changes the ID to avoid collision between left and right
  * This is for Extend operation to union meta (it does not alter the meta IDs)
  *
  * @param left_dataset  first metadata set
  * @param right_dataset second metadata set
  */
case class IRUnionAggMD(var left_dataset: MetaOperator, var right_dataset: MetaOperator,
                        left_ds_name: String = "right", right_ds_name: String = "left") extends MetaOperator {
  override def getDependencies: List[IROperator] = List(left_dataset, right_dataset)

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator = {
    var res = this.copy()
    var hasMatch = false
    if (oldDep == left_dataset && newDep.isMetaOperator) {
      res = res.copy(left_dataset = newDep.asInstanceOf[MetaOperator])
      hasMatch = true
    }

    if (oldDep == right_dataset && newDep.isMetaOperator) {
      res = res.copy(right_dataset = newDep.asInstanceOf[MetaOperator])
      hasMatch = true
    }

    if (hasMatch) res
    else throw new DependencyException
  }
}

/**
  * Computes the union of the two input datasets.
  * It changes the ID to avoid collision between left and right
  *
  * @param left_dataset        first regiondata set
  * @param right_dataset       second regiondata set
  * @param schema_reformatting it is the sorted list of field positions. It refers to the field in the right_dataset.
  *                            It can take value -1 if the corresponding field must be null.
  *                            E.g. if it is List[(1,3,2,-1)], then it means that in the output, all the regions produced
  *                            from the right_dataset will have as 1st value the 1st value of the right_dataset region,
  *                            as 2nd value the 3rd value, as 3rd value the 2nd one and as last value a null.
  */
case class IRUnionRD(schema_reformatting: List[Int], var left_dataset: RegionOperator,
                     var right_dataset: RegionOperator) extends RegionOperator {
  override def getDependencies: List[IROperator] = List(left_dataset, right_dataset)

  override def substituteDependency(oldDep: IROperator, newDep: IROperator): IROperator = {
    var res = this.copy()
    var hasMatch = false
    if (oldDep == left_dataset && newDep.isRegionOperator) {
      res = res.copy(left_dataset = newDep.asInstanceOf[RegionOperator])
      hasMatch = true
    }

    if (oldDep == right_dataset && newDep.isRegionOperator) {
      res = res.copy(right_dataset = newDep.asInstanceOf[RegionOperator])
      hasMatch = true
    }

    if (hasMatch) res
    else throw new DependencyException
  }
}