package it.polimi.genomics.importer.GMQLImporter

import it.polimi.genomics.importer.FileLogger.FileLogger
import it.polimi.genomics.repository.FSRepository.LFSRepository
import it.polimi.genomics.repository.GMQLRepository.GMQLSample

/**
  * Created by Nacho on 10/17/16.
  */
object GMQLLoader {

  /**
    * using information in the information, will insert into GMQLRepository the files
    * already downloaded, transformed and organized.
    *
    * Files have to be in the folder: information.outputFolder/dataset.outputFolder/Transformations/
    * and have to be sorted as pairs (file,file.meta) and also the .schema file have to be in the same folder.
    *
    * The process will look for every file ".meta" and will try to get its pair file.
    *
    * The GMQLTransformer should put inside the folder just the necessary files.
    *
    * @param source contains files location and datasets organization
    */
  def loadIntoGMQL(source: GMQLSource): Unit = {
    source.datasets.foreach(dataset =>{
      val log = new FileLogger(source.outputFolder + "/" + dataset.outputFolder + "/Transformations")

      val listAdd = new java.util.ArrayList[GMQLSample]()
      val listDelete = new java.util.ArrayList[GMQLSample]()

      log.filesToUpdate().filterNot(_.name.endsWith(".meta")).foreach(file => listAdd.add(GMQLSample(
        source.outputFolder + "/" + dataset.outputFolder + "/Transformations/" + file.name,
        source.outputFolder + "/" + dataset.outputFolder + "/Transformations/" + file.name+".meta",
        null)))

      log.filesToOutdate().filterNot(_.name.endsWith(".meta")).foreach(file => listDelete.add(GMQLSample(
        source.outputFolder + "/" + dataset.outputFolder + "/Transformations/" + file.name,
        source.outputFolder + "/" + dataset.outputFolder + "/Transformations/" + file.name+".meta",
        null)))

      if(listAdd.size()>0 || listDelete.size()>0) {
        val text = "GMQLDataset: " + dataset.outputFolder+", User: "+source.gmqlUser +
          (if(listAdd.size()>0){ ". Trying to add "+listAdd.size()+" samples" }else "") +
          (if(listDelete.size()>0){ ". Trying to delete "+listDelete.size()+" samples" }else "")
        println(text)
        //If the dataset exists already, I have to add sample instead of importing the whole dataset.
        //I have to use GMQLRepository.DSExists
        val repo = new LFSRepository
        //val repo = new DFSRepository
        //if(repo.DSExists())
        //if repo exists I do ADD/DELETE
        //ELSE IMPORT
        repo.importDs(
          source.name+" "+dataset.outputFolder,
          source.gmqlUser,
          listAdd,
          source.outputFolder+"/"+dataset.outputFolder+"/Transformations/"+dataset.outputFolder+".schema")
        println("import completed")
      }
      log.markAsProcessed()
      log.saveTable()
    })
  }
}
