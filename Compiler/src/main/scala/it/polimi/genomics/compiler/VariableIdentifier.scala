package it.polimi.genomics.compiler

import scala.util.parsing.input.Positional


abstract class Variable(val name:String)

case class VariableIdentifier(IDName : String) extends Variable(IDName) with Positional {}

case class VariablePath(path : String, parser_name : String) extends Variable(path) with Positional {}
