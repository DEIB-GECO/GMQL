package it.polimi.genomics.compiler

import it.polimi.genomics.GMQLServer.GmqlServer

import scala.util.parsing.input.Position

/**
 * Created by pietro on 16/09/15.
 */
@SerialVersionUID(21L)
case class MaterializeOperator(op_pos : Position,
                          input1 : Variable,
                          input2 : Option[Variable] = None,
                          parameters : OperatorParameters)
  extends Operator(op_pos,input1, input2, parameters) with Serializable {

  override val operator_name = "MATERIALIZE"
  override val accepted_named_parameters = List("")

  override def check_input_number = one_input

  if (!parameters.unamed.isDefined) {
    val msg = "Materialize statement at line " + op_pos.line + " requires a store path"
    throw new CompilerException(msg)
  }
  val sp = parser_unnamed(materializePath, None)
  var store_path : String = sp.get.path

  def translate_operator(status : CompilerStatus):CompilerDefinedVariable = {
    val super_var = status.getVariable(input1.asInstanceOf[VariableIdentifier].name).get
    status.get_server.setOutputPath(store_path).MATERIALIZE(super_var.payload)
    println("\n\n--- TESTING --- \nschema: " + super_var.payload.schema + "\n\n\n")
    super_var
  }
}
