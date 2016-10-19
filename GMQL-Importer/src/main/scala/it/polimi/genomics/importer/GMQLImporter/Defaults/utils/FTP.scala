package it.polimi.genomics.importer.GMQLImporter.Defaults.utils

//https://gist.github.com/owainlewis/06e8bdfa6c42acec2ef9dec756c05c2f
//I use as interface for FTP client.
//package io.forward.ftp

import java.io.{File, FileOutputStream, InputStream}

import org.apache.commons.net.ftp._

import scala.util.Try

final class FTP() {

  val client: FTPClient = new FTPClient
  def Client: FTPClient = client
  def login(username: String, password: String): Try[Boolean] = Try {
    client.login(username, password)
  }
  def reply: Int = client.getReplyCode
  def replyString: String = client.getReplyString

  def connect(host: String): Try[Unit] = Try {
    client.connect(host)
    client.enterLocalPassiveMode()
  }

  def connected: Boolean = client.isConnected

  def disconnect(): Unit = client.disconnect()

  def canConnect(host: String): Boolean = {
    client.connect(host)
    val connectionWasEstablished = connected
    client.disconnect()
    connectionWasEstablished
  }
  def listDirectories():Array[FTPFile]={
    client.listDirectories()
  }
  def listFiles(dir: Option[String] = None): List[FTPFile] =
    dir.fold(client.listFiles)(client.listFiles).toList

  def connectWithAuth(host: String,
                      username: String = "anonymous",
                      password: String = "") : Try[Boolean] = {
    for {
      connection <- connect(host)
      login      <- login(username, password)
    } yield login
  }

  def cd(path: String): Boolean =
    client.changeWorkingDirectory(path)

  def filesInCurrentDirectory: Seq[String] =
    listFiles().map(_.getName)

  def downloadFileStream(remote: String): InputStream = {
    val stream = client.retrieveFileStream(remote)
    client.completePendingCommand()
    stream
  }
  def workingDirectory(): String ={
    client.printWorkingDirectory()
  }

  def downloadFile(remote: String, local: String): Boolean = {
    val os = new FileOutputStream(new File(local))
    client.retrieveFile(remote, os)
  }

  def uploadFile(remote: String, input: InputStream): Boolean =
    client.storeFile(remote, input)
}