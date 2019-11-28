package it.polimi.genomics.core.DataStructures

object GMQLOperator extends  Enumeration {

  type GMQLOperators = Value

  val Undefined = Value("undefined")

  val Startup = Value("Startup")   // Fake operator to describe the startup operations of the execution engine
  val Shutdown = Value("Shutdown") // Fake operator to describe the shutdown operations of the execution engine

  val Select = Value("Select")
  val SelectMem = Value("SelectMem")

  val Cover = Value("Cover")
  val Histogtam = Value("Histogram")
  val Summit = Value("Summit")
  val Flat = Value("Flat")

  val Difference = Value("Difference")
  val Extend = Value("Extend")
  val Group = Value("Group")
  val Join = Value("Join")
  val Map = Value("Map")
  val Materialize =  Value("Materialize")
  val Merge = Value("Merge")
  val Order = Value("Order")
  val Project = Value("Project")
  val Union = Value("Union")




}
