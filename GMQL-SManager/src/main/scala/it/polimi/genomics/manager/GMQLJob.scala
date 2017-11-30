package it.polimi.genomics.manager

import it.polimi.genomics.core.GDMSUserClass.GDMSUserClass
import it.polimi.genomics.core.{GMQLSchemaCoordinateSystem, GMQLSchemaFormat}

/**
  *
  * @param jobId
  * @param queryPath
  * @param queryName
  * @param username
  * @param userClass
  * @param outputFormat
  * @param outputCoordinateSystem
  * @param applicationId
  * @param outputDatasets
  */
abstract class Job(jobId: String,
                   queryPath: String,
                   queryName: String,
                   username: String,
                   userClass: GDMSUserClass,
                   outputFormat: GMQLSchemaFormat.outputFormat,
                   outputCoordinateSystem: GMQLSchemaCoordinateSystem.outputFormat,
                   applicationId: String,
                   outputDatasets: List[String]
                  ) {



}