

Table of contents
===================
[TOC]

Engine Configurations 
===================

Two sets of configurations are set for **GMQL** Engine :
	
> - Repository configurations, set in <i class="icon-cog"></i> [repo.xml](GMQL-Repository/src/main/resources/GMQL.conf) file.  
> - Executors configurations  set in <i class="icon-cog"></i> [impl.xml](GMQL-SManager/src/main/resources/impl.conf) file. 
> 
Repository Configurations: 
-------------------------------------


Property Name  | Default  | Meaning
------------------ | ----------- | ----------
GMQL_REPO_TYPE|LOCAL| The type of repository considered for this installation. If the repository is set to **LOCAL**, the data will be all stored on the Local file system of the application node. If the mode is set to **HDFS**, the samples will be stored on Hadoop Distributed File System (HDFS). If the mode is set to **REMOTE**, the samples will be stored on Hadoop Distributed File System (HDFS) of a remote cluster. When the mode is set to **REMOTE**, LAUNCHER_MODE must be set to **REMOTE_CLUSTER**. In all cases, the configuration of **GMQL_HOME** should be set, since it will hold the datasets description files. `Mandatory configuration.`
GMQL_LOCAL_HOME |/tmp/repo/| Sets the home directory <i class="icon-folder-open"></i> for **GMQL** repository on the local file system of the Application server. Home directory of **GMQL** contains: the description of GMQL datasets<i class="icon-folder-open"></i> , the metadata for the datasets <i class="icon-folder-open"></i> , and the history of the execution. In case the **repository mode** was set to **Local**, the regions generated fromt he execution are stored in **regions** <i class="icon-folder-open"></i>  folder under the repository home directory. `Mandatory configuration.` 
GMQL_DFS_HOME | /user/repo/ | The home folder <i class="icon-folder-open"></i> of the  repository on HDFS. In case of **repository mode** set to **YARN** execution, the sample data are stored in the regions folder in HDFS under the repository home folder. `Considered only when GMQL_REPO is set to HDFS.`
[HADOOP_HOME](http://hadoop.apache.org/)|/usr/local/hadoop/|The location of Hadoop installation. `Considered only when GMQL_REPO is set to HDFS.`
HADOOP_CONF_DIR|/usr/local/hadoop/etc/hadoop/|The location of Hadoop configuration folder <i class="icon-folder-open"></i>. `Considered only when GMQL_REPO is set to HDFS.`
GMQL_CONF_DIR|$GMQL_INSTALLATION/conf|The folder location <i class="icon-folder-open"></i> of GMQL configuration files. 

Launcher Configurations: 
-------------------------------------

Property Name  | Default  | Meaning
------------------ | ----------- | ----------
LAUNCHER_MODE|LOCAL| The launcher execution mode, can be set to: **LOCAL**, **CLUSTER**, or **REMOTE_CLUSTER** .
SPARK_HOME | /usr/local/spark/ | Spark home directory <i class="icon-folder-open"></i>. `Considered only when LAUNCHER_MODE is set to CLUSTER or REMOTE_CLUSTER.`
CLI_JAR_NAME | GMQL-Cli-2.0-jar-with-dependencies.jar | Name of the fat Jar of Command Line Interface (CLI), this is used for the **cluster** launcher mode. `Considered only when LAUNCHER_MODE is set to CLUSTER or REMOTE_CLUSTER.`
CLI_CLASS|it.polimi.genomics.cli.GMQLExecuteCommand| The class path of the Command Line Interface of GMQL. Can be changed only by the developers, better to leave it on the defaults.
LIB_DIR_LOCAL | $GMQL_INSTALLATION/lib/| Location of the library folder, containing the repository jar, and the executable jars of GMQL. The default is inside the package installation folder.
LIB_DIR_HDFS | /user/repo/lib/ | Location on HDFS of the library folder, where the cli jar file is stored. `Considered only when LAUNCHER_MODE is set to CLUSTER or REMOTE_CLUSTER.`



> **Note:**

> - GMQL Engine can be connected to several repositories types; such as Local repository, [HDFS](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html) repository, or Remote repository. Currently, only one repository type can be set for each installation.
> - The genomic engine can run on several types of implementations. The supported implementations are: [Apache Spark](http://spark.apache.org/), [Apache Flink](https://flink.apache.org/), and [SciDB](http://www.paradigm4.com/). the default one is Spark.
> - Apache Spark, and Flink can have one of three Launchers: LOCAL, CLUSTER, and REMOTE_CLUSTER. The remote cluster is executed though calls through web services (enabled for Spark though [Livy](http://livy.io/)).

