  package it.polimi.genomics.compiler


  import it.polimi.genomics.core.DataStructures.RegionAggregate._

  import scala.util.parsing.input.Position

  @SerialVersionUID(17L)
  case class ProjectOperator(op_pos : Position,
                             input1 : Variable,
                             input2 : Option[Variable] = None,
                             output : VariableIdentifier,
                             parameters : OperatorParameters)
    extends Operator(op_pos,input1, input2, parameters)
    with BuildingOperator2 with Serializable {

    override val operator_name = "PROJECT"
    override val accepted_named_parameters = List("region_update","metadata_update","metadata")
    override def check_input_number = one_input
    var region_project_fields : Option[List[Int]] = None
    var meta_projection : Option[List[String]] = None
    var region_modifier : Option[List[RegionFunction]] = None

      override def preprocess_operator(status: CompilerStatus) : Boolean = {
        super_variable_left = Some(
          get_variable_if_defined(input1.asInstanceOf[VariableIdentifier].name,
          status).get.payload
        )

        if (parameters.unamed.isDefined) {
          val cond_proj: Option[Either[AllBut,
                                List[SingleProjectOnRegion]]] = parser_unnamed(region_project_cond, None)

          region_project_fields =
            cond_proj.get match {

              case Right(list_proj) => {

                val list_set : List[Set[Int]] = for (a <- list_proj) yield {
                  a.asInstanceOf[RegionProject].field match {
                    case FieldPosition(p) => {
                      left_var_check_num_field(p)
                      Set(p)
                    }
                    case FieldName(n) => {
                      Set(left_var_get_field_name(n).get)
                    }
                    case FieldNameWithWildCards(n) => {
                      left_var_get_field_name_wildcards(n).toSet
                    }
                  }
                }

                Some(
                  list_set.fold(Set.empty[Int])((x,y) => x.union(y))
                    .toList
                    .sorted
                )

              }

              case Left(ab) => {
                val pos : List[Set[Int]]= ab.fields.map {
                  x => x match {
                    case FieldPosition(p) => {
                      left_var_check_num_field(p)
                      (for (i <- 0 to super_variable_left.get.get_number_of_fields-1) yield i)
                        .filter(_ != p)
                        .toSet
                    }
                    case FieldName(n) => {
                      val p = left_var_get_field_name(n).get
                      (for (i <- 0 to super_variable_left.get.get_number_of_fields-1) yield i)
                        .filter(_ != p)
                        .toSet
                    }
                    case FieldNameWithWildCards(n) => {
                      val ps = left_var_get_field_name_wildcards(n).toSet
                      (for (i <- 0 to super_variable_left.get.get_number_of_fields-1) yield i)
                        .toSet
                        .diff(ps)
                    }
                  }}
                Some(
                  pos.fold(
                      (for (i <- 0 to super_variable_left.get.get_number_of_fields-1) yield i).toSet)
                      ((x,y) => x.intersect(y)
                    )
                    .toList
                    .sorted
                )
              }
            }

        }
        for (p <- parameters.named) {
          p.param_name.trim.toLowerCase() match {
            case "region_update" => {
              val partial = parser_named(region_modifier_list,p.param_name.trim, p.param_value.trim)
              region_modifier  = {
                val allNames = partial
                  .get
                  .filter(_.isInstanceOf[RegionModifier])
                  .map(_.asInstanceOf[RegionModifier].field)
                  .filter(_.isInstanceOf[FieldName])
                  .map(_.asInstanceOf[FieldName].name)
                if (allNames.toSet.size != allNames.size){
                  val msg = "In " + operator_name + " at line " + op_pos.line + ", " +
                    "in 'region_update' option some field name has been repeated multiple times"
                  throw new CompilerException(msg)
                }
                val l = partial.get
                  .filter(_.isInstanceOf[RegionModifier])
                  .map(x =>
                    status.get_server.implementation.regionExtensionFactory.get(
                      refineREDag(x.asInstanceOf[RegionModifier].dag),
                      x.asInstanceOf[RegionModifier].field match {
                        case FieldPosition(p) => {
                          left_var_check_num_field(p)
                          Right(p)}
                        case FieldName(n) => {
                          try {
                            val p = left_var_get_field_name(n).get
                            Right(p)
                          }catch {
                            case e:Exception => Left(n)
                          }
                        }
                      }
                    ))
                if (l.isEmpty) None else Some(l)
              }
            }
            case "metadata_update" => {
              throw new CompilerException("'meta_update' option for the PROJECT is not available yet")
            }
            case "metadata" => {
              meta_projection = parser_named(metadata_attribute_list,p.param_name.trim, p.param_value.trim)
            }
          }
        }

        true
      }

    override def translate_operator(status : CompilerStatus):CompilerDefinedVariable = {

      val projected = super_variable_left.get.PROJECT(
        meta_projection,
        None,
        region_project_fields,
        extended_values = region_modifier)

      CompilerDefinedVariable(
        output.name,
        output.pos, projected
      )
    }


    def refineREDag(dag:RENode) : RENode = {
      dag match {
        case REFieldNameOrPosition(f) => {
          if (f.isInstanceOf[FieldPosition]){
            val p = f.asInstanceOf[FieldPosition].pos
            left_var_check_num_field(p)
            REPos(p)
          }
          else  {
            val p = left_var_get_field_name(f.asInstanceOf[FieldName].name).get
            REPos(p)
          }
        }
        case READD(a,b) => READD(refineREDag(a), refineREDag(b))
        case RESUB(a,b) => RESUB(refineREDag(a), refineREDag(b))
        case REMUL(a,b) => REMUL(refineREDag(a), refineREDag(b))
        case REDIV(a,b) => REDIV(refineREDag(a), refineREDag(b))
        case _ => dag
      }
    }
  }

