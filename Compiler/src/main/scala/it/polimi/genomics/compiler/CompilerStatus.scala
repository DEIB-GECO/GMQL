package it.polimi.genomics.compiler

import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.core.DataStructures.IRVariable
import scala.util.parsing.input.Position

/**
 * Created by pietro on 21/09/15.
 */
case class CompilerDefinedVariable(name:String, pos:Position, payload:IRVariable)

class CompilerStatus(server : GmqlServer) {

  var defined_variables : List[CompilerDefinedVariable] = List.empty

  def getVariable(name_requested : String) : Option[CompilerDefinedVariable] = {
    val filtered = defined_variables.filter(x=>x.name.equals(name_requested))

    if (filtered.size == 0) None
    else Some(filtered(0))
  }

  @throws[CompilerException]
  def add_variable(name : String, pos:Position, variable : IRVariable) = {

    if (getVariable(name).isDefined) {
      val msg = "Compiler error: variable " + name + " redefinition"
      throw new CompilerException(msg)
    }
    defined_variables = defined_variables.:+(CompilerDefinedVariable(name,pos,variable))
  }

  def get_server = server

}
