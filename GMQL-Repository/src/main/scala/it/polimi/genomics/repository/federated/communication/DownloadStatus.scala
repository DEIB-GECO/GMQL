package it.polimi.genomics.repository.federated.communication

sealed abstract trait DownloadStatus

case class Pending() extends DownloadStatus
case class Downloading() extends DownloadStatus
case class Success() extends DownloadStatus
case class Failed(message: String) extends DownloadStatus
case class NotFound() extends DownloadStatus