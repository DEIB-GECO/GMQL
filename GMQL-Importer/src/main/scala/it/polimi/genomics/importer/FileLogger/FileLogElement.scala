package it.polimi.genomics.importer.FileLogger

/**
  * class to represent each file in the folder for the FileLog
  * @param name file name.
  * @param lastUpdate date of last update for the local file.
  * @param status action to be performed over the file.
  * @param origin file from where the file comes from.
  * @param originSize origin file size.
  * @param originLastUpdate  date of last update for the origin file.
  * @param dateProcessed date of last usage of the file (Either Transformed or Loaded).
  */
case class FileLogElement(name: String, lastUpdate: String, var status:FILE_STATUS.Value, origin: String, originSize: String, originLastUpdate: String, dateProcessed: String) {
}
