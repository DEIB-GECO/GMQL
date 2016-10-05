/* 
 * Copyright (C) 2014 Abdulrahman Kaitoua <abdulrahman.kaitoua at polimi.it>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package it.polimi.genomics.repository.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;

/**
 *
 * @author abdulrahman kaitoua <abdulrahman dot kaitoua at polimi dot it>
 */
public class Utilities {

    private static Utilities instance;
    private static final Logger logger = LoggerFactory.getLogger(Utilities.class);
    public static String USERNAME = "abdulrahman";
    public static final String HDFS = "MAPREDUCE";
    public static final String LOCAL = "LOCAL";
    public final String RepoDir;
    public String HDFSRepoDir = "";
    public final String MODE;
    public String CoreConfigurationFiles = "";
    public String HDFSConfigurationFiles = "";
    public final String GMQLHOME;
    private Configuration conf;
    private FileSystem fs;
    public static final FsPermission permission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);

    public static Utilities getInstance() {
        if (instance == null) {
            instance = new Utilities();
        }
        return instance;
    }
//    Utilities.getInstance().setClusterSettings( "/user/akaitoua/gmql_repo/","/etc/hadoop/conf/core-site.xml","/etc/hadoop/conf//hdfs-site.xml")
    public Utilities() {
        String gmql = System.getenv("GMQL_HOME");
        String user = System.getenv("USER");
        String dfs = System.getenv("GMQL_DFS_HOME");
        String exec = System.getenv("GMQL_EXEC");
        if(gmql == null)GMQLHOME="/Users/abdulrahman/gmql_repository"; else GMQLHOME = gmql;
        if(user == null )USERNAME = "gmql_user"; else USERNAME = user;
        if (dfs == null) this.HDFSRepoDir = "/user/akaitoua/gmql_repo/";else this.HDFSRepoDir = dfs;
        if (exec == null) {
            logger.warn("Environment variable GMQL_EXEC is empty... execution set to HDFS");
            this.MODE = LOCAL;
            //logger.warn("The execution is set to YARN execution since the value of the global variable { GMQL_EXEC } is not set properly .. ");
        } else this.MODE =/* "LOCAL" ;*/exec.toUpperCase();

        RepoDir = GMQLHOME + "/data/";
        String coreConf = System.getenv("HADOOP_CONF_DIR");
        String hdfsConf = System.getenv("HADOOP_CONF_DIR");
        CoreConfigurationFiles = (coreConf == null?/*"/usr/local/Cellar/hadoop/2.7.2/libexec/etc/hadoop/"*/"/etc/hadoop/conf/":coreConf) + "/core-site.xml";
        HDFSConfigurationFiles = (hdfsConf == null?/*"/usr/local/Cellar/hadoop/2.7.2/libexec/etc/hadoop/"*/"/etc/hadoop/conf/":hdfsConf) + "/hdfs-site.xml";
        logger.info(CoreConfigurationFiles);
        logger.info(HDFSConfigurationFiles);
//System.out.println("\n\n\n\n\n\n\n"+gmql+"\t"+GMQLHOME+"\t"+exec+"\t"+MODE+"\t"+user+"\t"+USERNAME+"\n\n\n\n\n\n");
        logger.info("GMQLHOME = " + gmql + "," + GMQLHOME);
        logger.info("MODE = " + exec + "," + MODE);
        logger.info("user = " + user + "," + USERNAME);
    }

    public void setClusterSettings(String hdfsHome,String core, String hdfs ){
        HDFSRepoDir = hdfsHome ;
        CoreConfigurationFiles = core ;
        HDFSConfigurationFiles = hdfs;

    }

    public static void setFullLocalPermissions(File file) {
        file.setExecutable(true, false);
        file.setReadable(true, false);
        file.setWritable(true, false);
    }
    /**
     *
     * @param sourceUrl
     * @param distUrl
     * @return
     * @throws IOException
     */
    public boolean copyfiletoHDFS(String sourceUrl, String distUrl) throws IOException {
        conf = gethdfsConfiguration();
        fs = FileSystem.get(conf);
        if (!fs.exists(new org.apache.hadoop.fs.Path(Paths.get(distUrl).getParent().toString()))) {
            fs.mkdirs(new org.apache.hadoop.fs.Path(Paths.get(distUrl).getParent().toString()));
            fs.setPermission(new org.apache.hadoop.fs.Path(Paths.get(distUrl).getParent().toString()), permission);
        }
        fs.copyFromLocalFile(new org.apache.hadoop.fs.Path(sourceUrl),
                new org.apache.hadoop.fs.Path(distUrl));
        if((new File(sourceUrl+".meta")).exists()){
            fs.copyFromLocalFile(new org.apache.hadoop.fs.Path(sourceUrl + ".meta"),
                    new org.apache.hadoop.fs.Path(distUrl+".meta"));
        }
        fs.setPermission(new org.apache.hadoop.fs.Path(distUrl), permission);
        return true;
    }

    /**
     *
     * @param sourceHDFSUrl
     * @param distLocalUrl
     * @return
     * @throws IOException
     */
    public boolean copyfiletoLocal(String sourceHDFSUrl, String distLocalUrl) throws IOException {
        conf = gethdfsConfiguration();
        fs = FileSystem.get(conf);

        if (!fs.exists(new org.apache.hadoop.fs.Path(sourceHDFSUrl))) {
            logger.error("The Dataset sample Url is not found: " + sourceHDFSUrl);
            return false;
        }
        if(fs.exists(new org.apache.hadoop.fs.Path(sourceHDFSUrl+".meta"))){
            fs.copyToLocalFile(new org.apache.hadoop.fs.Path(sourceHDFSUrl + ".meta"),
                    new Path(distLocalUrl + ".meta"));
        }
        fs.copyToLocalFile(new org.apache.hadoop.fs.Path(sourceHDFSUrl),
                new Path(distLocalUrl));
        File dist = new File(distLocalUrl);
        dist.setWritable(true, false);
        dist.setExecutable(true, false);
        dist.setReadable(true, false);
        return true;
    }

    public Configuration gethdfsConfiguration(){
        conf = new Configuration();
//        System.out.println(CoreConfigurationFiles);
//        System.out.println(HDFSConfigurationFiles);
        conf.addResource(new org.apache.hadoop.fs.Path(CoreConfigurationFiles));
        conf.addResource(new org.apache.hadoop.fs.Path(HDFSConfigurationFiles));
        conf.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        conf.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );
        return conf;
    }

    public FileSystem getFileSystem(){
        try {
            fs = FileSystem.get(gethdfsConfiguration());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fs;
    }

    /**
     *
     * @param HDFSsourceUrl
     * @param HDFSdistUrl
     * @return
     * @throws IOException
     */
    public boolean movefileInsideHDFS(String HDFSsourceUrl, String HDFSdistUrl) throws IOException {
        conf = gethdfsConfiguration();
        fs = FileSystem.get(conf);
        if (!fs.exists(new org.apache.hadoop.fs.Path(Paths.get(HDFSdistUrl).getParent().toString()))) {
            fs.mkdirs(new org.apache.hadoop.fs.Path(Paths.get(HDFSdistUrl).getParent().toString()));
            fs.setPermission(new org.apache.hadoop.fs.Path(Paths.get(HDFSdistUrl).getParent().toString()), permission);
        }

        if(fs.exists(new org.apache.hadoop.fs.Path(HDFSsourceUrl+".meta"))){
            fs.rename(new org.apache.hadoop.fs.Path(HDFSsourceUrl+".meta"),
                    new org.apache.hadoop.fs.Path(HDFSdistUrl));
        }

        fs.rename(new org.apache.hadoop.fs.Path(HDFSsourceUrl),
                new org.apache.hadoop.fs.Path(HDFSdistUrl));
        fs.setPermission(new org.apache.hadoop.fs.Path(HDFSdistUrl), permission);
        return true;
    }

    /**
     *
     * @param Url
     * @return
     * @throws IOException
     */
    public boolean createDirHDFS(String Url) throws IOException {
        conf = gethdfsConfiguration();
        fs = FileSystem.get(conf);
        if (!fs.exists(new org.apache.hadoop.fs.Path(Paths.get(Url).toString()))) {
            fs.mkdirs(new org.apache.hadoop.fs.Path(Url));
            fs.setPermission(new org.apache.hadoop.fs.Path(Url), permission);
        } else {
            return false;
        }
        return true;
    }

    /**
     *
     * @param url
     * @return
     * @throws IOException
     */
    public boolean deleteDFSDir(String url) throws IOException {
        conf = gethdfsConfiguration();
        fs = FileSystem.get(conf);
        fs.delete(new org.apache.hadoop.fs.Path(url), true);
        if(fs.exists(new org.apache.hadoop.fs.Path(url+".meta")))
            fs.delete(new org.apache.hadoop.fs.Path(url+".meta"), true);
        return true;
    }

    public void deleteFromLocalFSRecursive(String dir) {
        File f = new File(dir);
        File[] files =null;
        if (f.isDirectory()) {
            files = f.listFiles();
            if (!(files == null)) {
                for (File file : files) {
                    deleteFromLocalFSRecursive(file.toString());
                }
            }
            f.delete();
        }
        else
            f.delete();
    }

    /**
     *
     * @param url
     * @param username
     * @return
     * @throws IOException
     */
    public boolean deleteDFSSamples(String url, String username) throws IOException {
        conf = gethdfsConfiguration();
        fs = FileSystem.get(conf);
//        Path dir = new Path(Paths.get(url).getParent().toString());
        Path dir = (new Path(url)).getParent();

        fs.delete(new org.apache.hadoop.fs.Path(url), true);
        if(fs.exists(new org.apache.hadoop.fs.Path(url+".meta")))
            fs.delete(new org.apache.hadoop.fs.Path(url+".meta"), true);
        deleteEmptyFoldersRecursive( dir, username);

        return true;
    }

    private Path deleteEmptyFoldersRecursive( Path dir, String username) throws IOException {
        conf = gethdfsConfiguration();
        fs = FileSystem.get(conf);
        if (fs.isDirectory(dir) && fs.listStatus(dir).length == 0 && (!dir.toString().equals(HDFSRepoDir + username + "/regions")
                || !dir.toString().equals(conf.get("fs.defaultFS")+HDFSRepoDir + username + "/regions"))) {
            //System.out.println(HDFSRepoDir + USERNAME + "\tnot equals to\t" + dir.toString());
            Path d = dir.getParent();
            fs.delete(dir, true);
            deleteEmptyFoldersRecursive( d, username);
        } /*else {
            System.out.println("Does not satisfy the conditions");
        }*/
        return dir.getParent();
    }

    /**
     *
     * @param string
     * @return
     */
    public long hash(String string) {
        long h = 1125899906842597L; // prime
        int len = string.length();

        for (int i = 0; i < len; i++) {
            h = 31 * h + string.charAt(i);
        }
        return h;
    }

    /**
     *
     * @param inputfile
     */
    public void deleterecursive(File inputfile) {
        File files[] = inputfile.listFiles();
        for (File file : files) {
            if (file.isDirectory()) {
                deleterecursive(file);
            }
            System.out.println("INFO:  Folder" + file.getPath() + ", Status: " + (file.delete() ? "Done." : "Error"));
        }
    }

    public boolean checkDSNameinPublic(String DName) throws Exception
    {
        return checkDSName("public",DName);
    }

    public boolean checkDSNameinRepo(String username, String DName) throws Exception
    {
        return checkDSName(username,DName) || checkDSNameinPublic(DName);
    }
    /**
     *
     * @param DSName
     * @return
     */
    public boolean checkDSName(String username, String DSName) throws Exception{
        File dataSetDir = new File(RepoDir + username + "/datasets/");

//        System.out.println(RepoDir + username + "/datasets/");
        if (!dataSetDir.exists()) {
            logger.debug("This user may not be registered .."+ username);
            return false;
//            throw new Exception("This user may not be registered...");
        }
        File datasets[] = dataSetDir.listFiles();
        for (File dataset : datasets) {
            if (dataset.getName().equals(DSName.trim() + ".xml")) {
                //System.out.println(dataset.getName());
                return true;
            }
        }
        return false;
    }
}
