package it.polimi.genomics.importer.FileLogger

import java.util.Calendar

import scala.xml.XML
/**
  * Created by Nacho on 16/09/2016.
  */
/**
  * keeps a log file on the working folder called FileLog.xml
  * Log organization follows FileLogElement structure
  * @param path working folder.
  */
class FileLogger(path: String){

  if (!new java.io.File(path).exists) {
    new java.io.File(path).mkdirs()
  }
  if (!new java.io.File(path+ "/" + "FileLog.xml").exists()) {
    val elem = <file_list></file_list>
    XML.save(path + "/" + "FileLog.xml", elem)
  }
  var files: List[FileLogElement] = (XML.loadFile(path+ "/" + "FileLog.xml")\\"file").map(file =>
    FileLogElement(
      (file\"name").text,
      (file\"last_update").text,
      FILE_STATUS.withName((file\"status").text),
      (file\"origin").text,
      (file\"origin_size").text,
      (file\"origin_last_update").text,
      (file\"date_processed").text)).toList

  /**
    * checking with the log file decides if the file has to be updated/added or not.
    *
    * @param filename file name
    * @param origin origin file from the server
    * @param originSize file size from the server
    * @param originLastUpdate last modification for the file on the server
    * @return file has to be updated/added
    */
  def checkIfUpdate(filename: String, origin:String,originSize: String, originLastUpdate: String): Boolean= {
    //if the file already exists
    if (files.exists(_.name == filename)) {
      val oldFile = files.filter(_.name == filename).head
      //here I compare if size or modification date are different, but should compare hashs
      //also I check if the file already has an UPDATE or ADD status.
      if (oldFile.status == FILE_STATUS.UPDATE ||
        oldFile.status == FILE_STATUS.ADD)
        false
      else if(oldFile.originSize != originSize ||
        oldFile.originLastUpdate != originLastUpdate)
        true
      else {
        //this turns from "OUTDATED" to "NOTHING", was the only way I managed to
        //use properly the method markAsOutdated and note the files that were removed from the server
        files = files.map(file =>
          if (file.name == oldFile.name && file.status == FILE_STATUS.OUTDATED)
            FileLogElement(
              oldFile.name,
              oldFile.lastUpdate,
              FILE_STATUS.NOTHING,
              oldFile.origin,
              oldFile.originSize,
              oldFile.originLastUpdate,
              oldFile.dateProcessed)
          else file)
        false
      }
    }
    //is a new file, i have to add it to the log
    else {
      files = files :+ FileLogElement(filename, "", FILE_STATUS.ADD,origin,originSize,originLastUpdate,"")
      true
    }
  }

  /**
    * marks indicated file as to be ADD or UPDATED
    * @param filename name of the file to be marked
    */
  def markAsUpdated(filename:String): Unit ={
    //I check here if the file already has state as UPDATE or ADD, if it has, I do nothing.
    if(!files.exists(file =>
      file.name == filename &&
        !(file.status == FILE_STATUS.UPDATE ||
          file.status == FILE_STATUS.ADD))) {
      files = files.map(file =>
        if (file.name == filename)
          FileLogElement(
            file.name,
            Calendar.getInstance.getTime.toString,
            if (file.lastUpdate == "") FILE_STATUS.ADD else FILE_STATUS.UPDATE,
            file.origin,
            file.originSize,
            file.originLastUpdate,
            file.dateProcessed)
        else file)
    }
  }

  /**
    * returns a list with all the files to be updated by the next process.
    * returns files with status ADD or UPDATE
    * @return list with files that should be updated.
    */
  def filesToUpdate(): List[FileLogElement] ={
    files.filter(file => file.status == FILE_STATUS.ADD || file.status == FILE_STATUS.UPDATE)
  }
  /**
    * returns a list with all the files to be outdated by the next process.
    * returns files with status OUTDATE
    * @return list with files that should be outdated.
    */
  def filesToOutdate(): List[FileLogElement] ={
    files.filter(file => file.status == FILE_STATUS.ADD || file.status == FILE_STATUS.UPDATE)
  }
  /**
    * saves the full log into the FileLog.xml.
    */
  def saveTable(): Unit = {
    val log =
      <file_list>{files.map(file =>
        <file>{
            <name>{file.name}</name>
            <last_update>{file.lastUpdate}</last_update>
            <status>{file.status}</status>
            <origin>{file.origin}</origin>
            <origin_size>{file.originSize}</origin_size>
            <origin_last_update>{file.originLastUpdate}</origin_last_update>
            <date_processed>{file.dateProcessed}</date_processed>}
        </file>)}
      </file_list>
    XML.save(path + "/" + "FileLog.xml", log)
  }

  /**
    * mark all files in the log as outdated.
    * should be used when all the dataset is going to be checked against the new dataset.
    */
  def markAsOutdated(): Unit ={
    files = files.map(file =>
      if(file.status == FILE_STATUS.UPDATE || file.status == FILE_STATUS.ADD) file
      else FileLogElement(
        file.name,
        file.lastUpdate,
        FILE_STATUS.OUTDATED,
        file.origin,
        file.originSize,
        file.originLastUpdate,
        file.dateProcessed))
  }

  /**
    * updates the status of the files in the log in order to inform that was already processed.
    * turns "ADD" and "UPDATE" into "NOTHING"
    * updates dateProcessed
    */
  def markAsProcessed(): Unit ={
    files = files.map(file =>
      if(file.status == FILE_STATUS.ADD || file.status == FILE_STATUS.UPDATE)
        FileLogElement(file.name,
          file.lastUpdate,
          FILE_STATUS.NOTHING,
          file.origin,
          file.originSize,
          file.originLastUpdate,
          Calendar.getInstance.getTime.toString)
      else file)
  }
}



