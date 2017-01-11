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
        super_variable_left = Some(get_variable_if_defined(input1.asInstanceOf[VariableIdentifier].name, status).get.payload)

        if (parameters.unamed.isDefined) {
          val cond_proj: Option[Either[AllBut,List[SingleProjectOnRegion]]] = parser_unnamed(region_project_cond, None)

          region_project_fields =
            cond_proj.get match {

              case Right(list_proj) => Some(for (a <- list_proj) yield {

                a.asInstanceOf[RegionProject].field match {
                  case FieldPosition(p) => {
                    left_var_check_num_field(p)
                    p
                  }
                  case FieldName(n) => {
                    left_var_get_field_name(n).get
                  }
                }
              })

              case Left(ab) => {
                val pos = ab.field match {
                  case FieldPosition(p) => {
                    left_var_check_num_field(p)
                    p
                  }
                  case FieldName(n) => {
                    left_var_get_field_name(n).get
                  }
                }
                Some((for (i <- 0 to super_variable_left.get.get_number_of_fields-1) yield i).filter(_ != pos).toList)
              }
            }

        }
        for (p <- parameters.named) {
          p.param_name.trim.toLowerCase() match {
            case "region_update" => {
              val partial = parser_named(region_modifier_list,p.param_name.trim, p.param_value.trim)
              region_modifier  = {
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
      CompilerDefinedVariable(
        output.name,
        output.pos, super_variable_left.get.PROJECT(meta_projection ,None,region_project_fields,region_modifier)
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

