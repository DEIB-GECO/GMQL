package it.polimi.genomics.core.DataStructures

import it.polimi.genomics.core.DataStructures.CoverParameters.CoverFlag.CoverFlag
import it.polimi.genomics.core.DataStructures.CoverParameters.CoverParam
import it.polimi.genomics.core.DataStructures.ExecutionParameters.BinningParameter
import it.polimi.genomics.core.DataStructures.GroupMDParameters.Direction._
import it.polimi.genomics.core.DataStructures.GroupMDParameters.{NoTop, TopParameter}
import it.polimi.genomics.core.DataStructures.GroupRDParameters.{FIELD, GroupingParameter}
import it.polimi.genomics.core.DataStructures.JoinParametersRD.RegionBuilder.RegionBuilder
import it.polimi.genomics.core.DataStructures.JoinParametersRD.{JoinQuadruple, RegionBuilder}
import it.polimi.genomics.core.DataStructures.MetaAggregate.{MetaAggregateFunction, MetaExtension}
import it.polimi.genomics.core.DataStructures.MetaGroupByCondition.MetaGroupByCondition
import it.polimi.genomics.core.DataStructures.MetaJoinCondition.{AttributeEvaluationStrategy, MetaJoinCondition}
import it.polimi.genomics.core.DataStructures.MetadataCondition.MetadataCondition
import it.polimi.genomics.core.DataStructures.RegionAggregate._
import it.polimi.genomics.core.DataStructures.RegionCondition.{MetaAccessor, RegionCondition}
import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
//import org.apache.log4j.Logger

/** Class for representing a GMQL variable.
  *
  * @param metaDag   Dag (series of operations) to build the metadata of the variable
  * @param regionDag Dag (series of operations) to build the region data of the variable
  * @param schema    the schema of the new generate variable
  */
@SerialVersionUID(1000L)
case class IRVariable(metaDag: MetaOperator, regionDag: RegionOperator,
                      schema: List[(String, PARSING_TYPE)] = List.empty,
                      dependencies: List[IRVariable] = List(),
                      name: String = "")(implicit binS: BinningParameter) extends Serializable {

  override def toString: String = {
    metaDag match {
      case IRReadMD(_, _, d) => "READ "  + d
      case IRStoreMD(_, _, d) => "STORE " + d
      case _ => this.name
    }
  }

  def sources: Set[IRDataSet] = this.metaDag.sources union this.regionDag.sources
  def sourceInstances: Set[GMQLInstance] = this.sources.map(_.instance)

  def SELECT(meta_con: MetadataCondition): IRVariable = {
    add_select_statement(external_meta = None, semi_join_condition = None, meta_condition = Some(meta_con), region_condition = None)
      .copy(dependencies = List(this), name = "SELECT")
  }

  def SELECT(reg_con: RegionCondition): IRVariable = {
    add_select_statement(external_meta = None, semi_join_condition = None, meta_condition = None, region_condition = Some(reg_con))
      .copy(dependencies = List(this), name = "SELECT")
  }

  def SELECT(meta_con: MetadataCondition, reg_con: RegionCondition): IRVariable = {
    add_select_statement(external_meta = None, semi_join_condition = None, meta_condition = Some(meta_con), region_condition = Some(reg_con))
      .copy(dependencies = List(this), name = "SELECT")
  }

  def SELECT(semi_con: MetaJoinCondition, meta_join_variable: IRVariable): IRVariable = {
    add_select_statement(external_meta = Some(meta_join_variable.metaDag), semi_join_condition = Some(semi_con), meta_condition = None, region_condition = None)
      .copy(dependencies = List(this, meta_join_variable), name = "SELECT")
  }

  def SELECT(semi_con: MetaJoinCondition, meta_join_variable: IRVariable, meta_con: MetadataCondition): IRVariable = {
    add_select_statement(external_meta = Some(meta_join_variable.metaDag), semi_join_condition = Some(semi_con), meta_condition = Some(meta_con), region_condition = None)
      .copy(dependencies = List(this, meta_join_variable), name = "SELECT")
  }

  def SELECT(semi_con: MetaJoinCondition, meta_join_variable: IRVariable, reg_con: RegionCondition): IRVariable = {
    add_select_statement(external_meta = Some(meta_join_variable.metaDag), semi_join_condition = Some(semi_con), meta_condition = None, region_condition = Some(reg_con))
      .copy(dependencies = List(this, meta_join_variable), name = "SELECT")
  }

  def SELECT(semi_con: MetaJoinCondition, meta_join_variable: IRVariable, meta_con: MetadataCondition, reg_con: RegionCondition): IRVariable = {
    add_select_statement(external_meta = Some(meta_join_variable.metaDag), semi_join_condition = Some(semi_con), meta_condition = Some(meta_con), region_condition = Some(reg_con))
      .copy(dependencies = List(this, meta_join_variable), name = "SELECT")
  }

  /**
    * Return a new variable that is a the SELECTION of the class variable
    *
    * @param external_meta       The meta dataset to be used for the semi-join
    * @param semi_join_condition The semi-join condition
    * @param meta_condition      the condition on meta data
    * @param region_condition    the condition on region data
    * @return a new IRVariable
    */
  def add_select_statement(external_meta: Option[MetaOperator], semi_join_condition: Option[MetaJoinCondition],
                           meta_condition: Option[MetadataCondition], region_condition: Option[RegionCondition]): IRVariable = {


    var metaset_for_SelectRD: Option[MetaOperator] = None
    var new_meta_dag = this.metaDag
    var new_region_dag = this.regionDag
    if (meta_condition.isDefined) {
      new_meta_dag = IRSelectMD(meta_condition.get, new_meta_dag)
      metaset_for_SelectRD = Some(new_meta_dag)
      if (!(semi_join_condition.isDefined || region_condition.isDefined)) {
        new_region_dag = IRSelectRD(None, Some(new_meta_dag), this.regionDag)
      }
    }
    if (semi_join_condition.isDefined) {
      new_meta_dag = IRSemiJoin(external_meta.get, semi_join_condition.get, new_meta_dag)
      metaset_for_SelectRD = Some(new_meta_dag)
      if (region_condition.isEmpty) {
        new_region_dag = IRSelectRD(None, Some(new_meta_dag), this.regionDag)
      }
    }
    if (region_condition.isDefined) {
      if (metaset_for_SelectRD.isEmpty && uses_metadata(region_condition.get)) metaset_for_SelectRD = Some(new_meta_dag)
      new_region_dag = IRSelectRD(region_condition, metaset_for_SelectRD, regionDag)
    } else {

    }
    if (region_condition.isDefined && meta_condition.isEmpty && semi_join_condition.isEmpty) {
      new_meta_dag = this.metaDag
      new_region_dag = IRSelectRD(region_condition, Some(new_meta_dag), this.regionDag)
    }

    if (region_condition.isDefined)
      new_meta_dag = IRPurgeMD(new_region_dag, new_meta_dag)

    IRVariable(new_meta_dag, new_region_dag, this.schema)

  }

  def PROJECT(projected_meta: Option[List[String]] = None,
              extended_meta: Option[List[MetaExtension]] = None,
              all_but_meta: Boolean = false,
              projected_values: Option[List[Int]] = None,
              all_but_reg: Option[List[String]] = None,
              extended_values: Option[List[RegionFunction]] = None): IRVariable = {


    val new_projected_values = if (all_but_reg.isDefined)
      Some(this.schema.zipWithIndex.filter(x => !all_but_reg.get.contains(x._1._1)).map(_._2))
    else projected_values

    var new_meta_dag = this.metaDag
    if (projected_meta.isDefined || extended_meta.isDefined) {
      new_meta_dag = IRProjectMD(projected_meta, extended_meta, all_but_meta, new_meta_dag)
    }
    if (new_projected_values.isDefined || extended_values.isDefined) {

      val all_proj_values: Option[List[Int]] =
        if (new_projected_values.isDefined) {
          val list = new_projected_values.get
          val new_list =
            if (extended_values.isDefined) {
              list ++ ((this.schema.size) to (this.schema.size + extended_values.get.filter(x =>
                !x.output_index.isDefined || x.output_index.get >= 0
              ).size - 1)).toList
            }
            else {
              list
            }
          Some(new_list)
        } else {
          None
        }

      val new_region_dag = IRProjectRD(
        all_proj_values,
        extended_values,
        this.regionDag, this.metaDag)

      val modified_fields = extended_values
        .getOrElse(List.empty)
        .filter(_.output_index.isDefined)
        .map(x => (x.output_index, x.asInstanceOf[RegionExtension].out_type))

      val schema_retyped =
        for (i <- 0 to this.schema.size - 1) yield {
          val new_type = modified_fields.filter(_._1 == Some(i))
          if (!new_type.isEmpty) {
            (this.schema(i)._1, new_type(0)._2)
          }
          else {
            this.schema(i)
          }
        }

      val new_schema_pt1 =
        if (new_projected_values.isDefined) {
          for (x <- new_projected_values.get) yield schema_retyped(x)
        }
        else {
          schema_retyped
        }


      val new_schema = new_schema_pt1.toList ++
        extended_values.getOrElse(List.empty)
          .filter(_.output_name.isDefined)
          .map(x =>
            (
              x.output_name.get,
              if (x.isInstanceOf[RegionExtension])
                x.asInstanceOf[RegionExtension].out_type
              else
                ParsingType.DOUBLE
            )
          )

      IRVariable(new_meta_dag, new_region_dag, new_schema, List(this), name = "PROJECT")

    } else {

      val new_region_dag = if (!(projected_meta.isDefined || extended_meta.isDefined)) IRProjectRD(
        projected_values,
        extended_values,
        this.regionDag, this.metaDag)
      else this.regionDag

      new IRVariable(
        new_meta_dag,
        new_region_dag,
        if (projected_meta.isDefined || extended_meta.isDefined) this.schema else List.empty, List(this), name = "PROJECT")

    }

  }

  def EXTEND(region_aggregates: List[RegionsToMeta]): IRVariable = {
    new IRVariable(IRUnionAggMD(this.metaDag, IRAggregateRD(region_aggregates, this.regionDag)),
      this.regionDag,
      this.schema, List(this), name = "EXTEND")
  }

  /** Group by with both meta grouping and region grouping
    */
  def GROUP(meta_keys: Option[MetaGroupByCondition] = None,
            meta_aggregates: Option[List[MetaAggregateFunction]] = None,
            meta_group_name: String = "_group",
            region_keys: Option[List[GroupingParameter]],
            region_aggregates: Option[List[RegionsToRegion]]): IRVariable = {

    //only region grouping
    if (!meta_keys.isDefined && !meta_aggregates.isDefined) {

      val new_schema = region_keys
        .getOrElse(List.empty)
        .filter(_.asInstanceOf[FIELD].position >= 0)
        .map(_.asInstanceOf[FIELD].position)
        .map(schema(_)) ++
        region_aggregates
          .getOrElse(List.empty)
          .map(x => (x.output_name.get, x.resType))

      new IRVariable(
        this.metaDag,
        IRGroupRD(region_keys, region_aggregates, this.regionDag),
        new_schema, List(this)
      )
    }
    //only the metadata grouping
    else if (!region_keys.isDefined && !region_aggregates.isDefined) {
      new IRVariable(
        IRGroupMD(
          meta_keys.getOrElse(new MetaGroupByCondition(List.empty)),
          meta_aggregates,
          meta_group_name,
          this.metaDag,
          this.regionDag),
        this.regionDag,
        this.schema, List(this), name = "GROUP"
      )
    }
    else {
      val new_schema = region_keys
        .getOrElse(List.empty)
        .filter(_.asInstanceOf[FIELD].position >= 0)
        .map(_.asInstanceOf[FIELD].position)
        .map(schema(_)) ++
        region_aggregates
          .getOrElse(List.empty)
          .map(x => (x.output_name.get, x.resType))

      val new_region_dag = IRGroupRD(region_keys, region_aggregates, this.regionDag)
      val new_meta_dag = IRGroupMD(
        meta_keys.getOrElse(new MetaGroupByCondition(List.empty)),
        meta_aggregates,
        meta_group_name,
        this.metaDag,
        new_region_dag)
      new IRVariable(
        new_meta_dag,
        new_region_dag,
        new_schema, List(this), name = "GROUP")

    }

  }

  /** ORDER with both metadata and region sorting
    */
  def ORDER(meta_ordering: Option[List[(String, Direction)]] = None, meta_new_attribute: String = "_group", meta_top_par: TopParameter = NoTop(),
            region_ordering: Option[List[(Int, Direction)]], region_top_par: TopParameter = NoTop()): IRVariable = {

    //only region ordering
    if (!meta_ordering.isDefined) {
      val new_schema = this.schema ++ List(("order", ParsingType.INTEGER))
      new IRVariable(this.metaDag, IROrderRD(region_ordering.get, region_top_par, this.regionDag), new_schema, List(this), name = "ORDER")
    }
    //only metadata ordering
    else if (!region_ordering.isDefined) {
      val new_meta_dag = new IROrderMD(meta_ordering.get, meta_new_attribute, meta_top_par, this.metaDag)
      val new_region_dag = new IRPurgeRD(new_meta_dag, this.regionDag)

      new IRVariable(new_meta_dag, new_region_dag, this.schema, List(this), name = "ORDER")
    }
    else {
      val new_meta_dag = IROrderMD(meta_ordering.get, meta_new_attribute, meta_top_par, this.metaDag)
      val new_region_dag = IRPurgeRD(new_meta_dag, IROrderRD(region_ordering.get, region_top_par, this.regionDag))

      val new_schema = this.schema ++ List(("order", ParsingType.INTEGER))
      new IRVariable(new_meta_dag, new_region_dag, new_schema, List(this), name = "ORDER")
    }
  }


  def COVER(flag: CoverFlag, minAcc: CoverParam, maxAcc: CoverParam, aggregates: List[RegionsToRegion], groupBy: Option[List[AttributeEvaluationStrategy]]): IRVariable = {

    val grouping: Option[MetaGroupOperator] =
      if (groupBy.isDefined) {
        Some(IRGroupBy(MetaGroupByCondition.MetaGroupByCondition(groupBy.get), this.metaDag))
      } else {
        None
      }

    val new_region_operator = IRRegionCover(flag, minAcc, maxAcc, aggregates, grouping, this.regionDag)
    new_region_operator.binSize = binS.size

    val new_meta = IRCollapseMD(grouping, this.metaDag)

    val new_schema = List(("AccIndex", ParsingType.INTEGER),
      ("JaccardIntersect", ParsingType.DOUBLE),
      ("JaccardResult", ParsingType.DOUBLE)) ++
      (aggregates.map(x => new_schema_field(x.output_name.getOrElse("unknown"), x.resType)))
        .foldLeft(List.empty[(String, PARSING_TYPE)])(_ ++ _)
    new IRVariable(new_meta, new_region_operator, new_schema, List(this), name = "COVER")
  }

  /**
    * Return a new field nested into a field, to be added to the schema of the variable
    *
    * @param name     the new name
    * @param parsType the type
    */
  def new_schema_field(name: String, parsType: PARSING_TYPE) = {
    List((name, parsType))
  }

  def MERGE(groupBy: Option[List[AttributeEvaluationStrategy]]): IRVariable = {
    if (!groupBy.isDefined) {
      new IRVariable(IRMergeMD(this.metaDag, None), IRMergeRD(this.regionDag, None), this.schema, List(this), name = "MERGE")
    }
    else {
      val grouping = Some(IRGroupBy(MetaGroupByCondition.MetaGroupByCondition(groupBy.get), this.metaDag))
      new IRVariable(IRMergeMD(this.metaDag, grouping), IRMergeRD(this.regionDag, grouping), this.schema, List(this), name = "MERGE")
    }
  }

  //Difference A B: is translated to A.DIFFERENCE(B)
  def DIFFERENCE(condition: Option[MetaJoinCondition] = None,
                 subtrahend: IRVariable,
                 is_exact: Boolean = false) = {

    val meta_join_cond = if (condition.isDefined) {
      SomeMetaJoinOperator(IRJoinBy(condition.get, this.metaDag, subtrahend.metaDag))
    }
    else {
      NoMetaJoinOperator(
        IRJoinBy(it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition(List.empty),
          this.metaDag, subtrahend.metaDag))
    }

    val new_region_dag = IRDifferenceRD(meta_join_cond, this.regionDag, subtrahend.regionDag, is_exact)

    new_region_dag.binSize = binS.size
    val new_meta_dag = /*this.metaDag*/
      IRDiffCombineMD(meta_join_cond,
        this.metaDag,
        subtrahend.metaDag,
        "",
        "")

    //difference does not change the schema
    IRVariable(new_meta_dag, new_region_dag, this.schema, List(this, subtrahend), name = "DIFFERENCE")
  }

  def UNION(right_dataset: IRVariable, left_name: String = "", right_name: String = ""): IRVariable = {

    val schema_reformatting: List[Int] = for (f <- this.schema) yield {
      right_dataset.get_field_by_name(f._1).getOrElse(-1)
    }

    IRVariable(IRUnionMD(right_dataset.metaDag, this.metaDag, left_name, right_name),
      IRUnionRD(schema_reformatting, right_dataset.regionDag, this.regionDag),
      this.schema, List(this, right_dataset), name = "UNION")

  }

  def get_field_by_name(name: String): Option[Int] = {
    val putative_position = this.schema.indexWhere(x => x._1.equals(name))
    if (putative_position >= 0)
      Some(putative_position)
    else
      None
  }

  def JOIN(meta_join: Option[MetaJoinCondition],
           region_join_condition: List[JoinQuadruple],
           region_builder: RegionBuilder,
           right_dataset: IRVariable,
           reference_name: Option[String] = None,
           experiment_name: Option[String] = None,
           join_on_attributes: Option[List[(Int, Int)]] = None): IRVariable = {

    if (region_join_condition.isEmpty &&
      !(region_builder == RegionBuilder.LEFT_DISTINCT ||
        region_builder == RegionBuilder.LEFT ||
        region_builder == RegionBuilder.RIGHT_DISTINCT ||
        region_builder == RegionBuilder.RIGHT ||
        region_builder == RegionBuilder.BOTH)) {
      throw new Exception("JOIN operator: if a join condition on distance is not provided, " +
        "then neither INTERSECTION nor CONCATENATION are allowed as output region builder.")
      sys.exit(1)
    }

    val new_meta_join_result = if (meta_join.isDefined) {
      SomeMetaJoinOperator(IRJoinBy(meta_join.get, this.metaDag, right_dataset.metaDag))
    } else {
      NoMetaJoinOperator(IRJoinBy(it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition(List.empty), this.metaDag, right_dataset.metaDag))
    }

    val new_meta_dag = IRCombineMD(new_meta_join_result,
      this.metaDag,
      right_dataset.metaDag,
      Some(region_builder),
      reference_name.getOrElse("left"),
      experiment_name.getOrElse("right"))

    val new_region_dag = IRGenometricJoin(
      new_meta_join_result,
      region_join_condition,
      region_builder,
      join_on_attributes,
      this.regionDag,
      right_dataset.regionDag)

    new_region_dag.binSize = binS.size

    val new_schema = region_builder match {
      case RegionBuilder.LEFT_DISTINCT => this.schema
      case RegionBuilder.RIGHT_DISTINCT => right_dataset.schema
      case RegionBuilder.BOTH => this.schema.map(x => (reference_name.getOrElse("left") + "." + x._1, x._2)) ++
        List(
          ("chr", ParsingType.STRING),
          ("start", ParsingType.LONG),
          ("stop", ParsingType.LONG),
          ("strand", ParsingType.CHAR)).map(x => (experiment_name.getOrElse("right") + "." + x._1, x._2)) ++
        right_dataset.schema.map(x => (experiment_name.getOrElse("right") + "." + x._1, x._2))
      case _ => this.schema.map(x => (reference_name.getOrElse("left") + "." + x._1, x._2)) ++
        right_dataset.schema.map(x => (experiment_name.getOrElse("right") + "." + x._1, x._2))
    }

    IRVariable(new_meta_dag, new_region_dag, new_schema, List(this, right_dataset), name = "JOIN")
  }

  def MAP(condition: Option[MetaJoinCondition.MetaJoinCondition],
          aggregates: List[RegionsToRegion],
          experiments: IRVariable,
          reference_name: Option[String] = None,
          experiment_name: Option[String] = None,
          count_name: Option[String] = None) = {

    apply_genometric_map(
      condition,
      aggregates,
      experiments,
      reference_name,
      experiment_name,
      count_name
    )
  }

  /**
    * It uses the current variable as reference and uses it to map the experiments passed as last parameters. Returns a new variable.
    *
    * @param condition   it is the metadata condition
    * @param aggregates  a list of aggregates functions; can be empty
    * @param experiments the dataset to be mapped
    */
  def apply_genometric_map(condition: Option[MetaJoinCondition.MetaJoinCondition],
                           aggregates: List[RegionsToRegion],
                           experiments: IRVariable,
                           reference_name: Option[String],
                           experiment_name: Option[String],
                           count_name_opt: Option[String]): IRVariable = {
    val new_join_result: OptionalMetaJoinOperator = if (condition.isDefined) {
      SomeMetaJoinOperator(IRJoinBy(condition.get, this.metaDag, experiments.metaDag))
    } else {
      NoMetaJoinOperator(IRJoinBy(it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition(List.empty), this.metaDag, experiments.metaDag))
    }

    val new_meta_dag = IRCombineMD(new_join_result,
      this.metaDag,
      experiments.metaDag,
      None,
      reference_name.getOrElse("left"),
      experiment_name.getOrElse("right"))
    val new_region_dag = IRGenometricMap(new_join_result, aggregates, this.regionDag, experiments.regionDag)
    new_region_dag.binSize = binS.size

    val count_name = count_name_opt
      .getOrElse("count_" + reference_name.getOrElse("left") + "_" + experiment_name.getOrElse("right"))

    val new_schema = this.schema ++
      (aggregates.map(x => new_schema_field(x.output_name.getOrElse("unknown"), x.resType)))
        .foldLeft(new_schema_field(count_name, ParsingType.DOUBLE))(_ ++ _)
    IRVariable(new_meta_dag, new_region_dag, new_schema, List(this, experiments), name = "MAP")
  }

  def uses_metadata(rc: RegionCondition): Boolean = {
    rc match {
      case RegionCondition.Predicate(_, _, MetaAccessor(_)) => true
      case RegionCondition.Predicate(_, _, _) => false
      case RegionCondition.NOT(x) => uses_metadata(x)
      case RegionCondition.AND(x, y) => uses_metadata(x) || uses_metadata(y)
      case RegionCondition.OR(x, y) => uses_metadata(x) || uses_metadata(y)
      case _ => false
    }
  }

  def get_number_of_fields = this.schema.size

  def get_type_by_name(name: String): Option[PARSING_TYPE] = {
    val putative_position = this.schema.indexWhere(x => x._1.equals(name))
    if (putative_position >= 0)
      Some(this.schema(putative_position)._2)
    else
      None
  }

  def get_field_by_name_with_wildcard(name: String): List[Int] = {
    val scaffold = name.replace(".", "\\.").replace("?", ".*")

    this
      .schema
      .indices
      .filter(x => this.schema(x)._1.matches(scaffold))
      .toList

  }

}
