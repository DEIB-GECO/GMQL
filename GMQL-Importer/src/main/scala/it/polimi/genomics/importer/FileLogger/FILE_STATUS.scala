package it.polimi.genomics.importer.FileLogger

/**
  * Represents the current action to be done over a file.
  */
object FILE_STATUS extends Enumeration {
  type FILE_STATUS = Value
  val ADD = Value("ADD")  //FILE DOES NOT EXIST AND HAS TO BE ADDED (EITHER DOWNLOADED OR TRANSFORMED)
  val UPDATE = Value("UPDATE") //FILE EXISTS BUT HAS TO BE UPDATED (EITHER DOWNLOADED OR TRANSFORMED)
  val OUTDATED = Value("OUTDATED") //LOCAL FILE EXISTS BUT IN ORIGIN DOES NOT EXIST ANYMORE HAS TO BE DELETED
  val NOTHING = Value("NOTHING") //FILE IS IN IT'S CORRECT STATE. NOTHING TO BE DONE
}
