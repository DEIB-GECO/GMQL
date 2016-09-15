package it.polimi.genomics.scidbapi.script

import it.polimi.genomics.scidbapi.exception.UnsupportedOperationSciException
import it.polimi.genomics.scidbapi.schema.{Attribute, Dimension}
import it.polimi.genomics.scidbapi.script.SciCommandType.SciCommandType


/**
  * The statement class define the interface for every possible
  * statement inside a script
  */
abstract class SciStatement
{
  def getStatementQuery() : String
}


// ------------------------------------------------------------
// ------------------------------------------------------------


/**
  * This class define comments for a SciDB script
  *
  * @param comment comment string, if on multiple lines the comment
  *                generated add "--" for each line
  */
case class SciComment(comment:String) extends SciStatement
{
  override def getStatementQuery(): String =
    comment.split("\n").map((s:String) => if(s == "") "" else "-- " + s).mkString("\n")
}


// ------------------------------------------------------------
// ------------------------------------------------------------


/**
  * Defines the enumeration of the possible commands
  */
object SciCommandType extends Enumeration
{
  type SciCommandType = Value

  val set_fetch = Value("set fetch")
  val set_no_fetch = Value("set no fetch")

  val set_timer = Value("set timer")
  val set_no_timer = Value("set no timer")

  val set_verbose = Value("set verbose")
  val set_no_verbose = Value("set no verbose")
}

/**
  * This class defines commands to setup the iquery client
  *
  * @param command iquery coomand
  */
case class SciCommand(command:SciCommandType) extends SciStatement
{
  override def getStatementQuery(): String = command.toString
}


// ------------------------------------------------------------
// ------------------------------------------------------------

/**
  * This class defines operations in SciDB to manage the stored arrays
 *
  * @param operation operation name
  * @param args opeartion arguments
  */
case class SciOperation(operation:String, args:Any*) extends SciStatement
{
  override def getStatementQuery(): String = (operation, args.toList) match
  {
    case ("load_library", (libname:String) :: Nil) => "load_library('" + libname + "')"
    case ("load_module", (modulename:String) :: Nil) => "load_module('" + modulename + "')"
    case ("remove", (anchor:String) :: Nil) => "remove(" + anchor + ")"
    case ("scan", (anchor:String) :: Nil) => "scan(" + anchor + ")"
    case ("show", (anchor:String) :: Nil) => "show(" + anchor + ")"
    case ("unload_library", (libname:String) :: Nil) => "unload_library('" + libname + "')"

    // list operation
    case ("list", (arg:String) :: Nil)
        if(List("aggregates","arrays","datastores","functions","instances","libraries","macros","operators","queries","types","users").contains(arg))
      => "list("+ arg +")"

    // default
    case (op, args) => throw new UnsupportedOperationSciException("Operation '"+ op +"("+ args.mkString(",") +")' not supported")
  }
}

// ------------------------------------------------------------
// ------------------------------------------------------------

/**
  * This class provide the command to create a new array inside SciDB
  *
  * @param temp true if the array to create should be temporary
  * @param name destination array name
  * @param dimensions desired dimensions
  * @param attributes desired attributes
  */
case class SciCreate(temp : Boolean,
                     name : String,
                     dimensions:List[Dimension],
                     attributes:List[Attribute])
  extends SciStatement
{
  override def getStatementQuery(): String =
  {
    "create "+ (if(temp) "temp " else "") +"array " + name +
      "<" + attributes.map(_.toString()).mkString(", ") + ">" +
      "[" + dimensions.map(_.toString()).mkString(", ") + "];"
  }
}