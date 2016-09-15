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

package it.polimi.genomics.repository.datasets;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;
import it.polimi.genomics.repository.Index.*;
import it.polimi.genomics.repository.util.*;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author abdulrahman kaitoua <abdulrahman dot kaitoua at polimi dot it>
 */
@XmlRootElement
@XmlType(name = "dataset")
@XmlAccessorType(XmlAccessType.FIELD)
public class GMQLDataSet {
    private final static Logger logger = LoggerFactory.getLogger(GMQLDataSet.class);

    @XmlTransient
    private final String MAPREDUCE = "MAPREDUCE";
    @XmlTransient
    private final String LOCAL = "LOCAL";

    @XmlTransient
    private Utilities utilities = Utilities.getInstance();
    
    @XmlAttribute(name = "name", required = true)
    private String DataSetName;
    
    @XmlAttribute(name = "schemaDir", required = true)
    private String schemaDir;
    
    @XmlAttribute(name = "GMQLUrl")
    private String GMQLScriptUrl;
    
    @XmlAttribute(name = "execType", required = true)
    private String execType;
    
    @XmlElement(name = "url", required = true)
    private List<GMQLDataSetUrlField> urls;
    
    @XmlTransient
    private boolean DScreateError = false;
    
    @XmlAttribute(name = "username", required = true)
    private String username;
    /**
     *
     */
    public GMQLDataSet() {
        this.username = "Abdo";
        this.DataSetName = "debug";
        this.schemaDir = "debug";
        this.setExecType(LOCAL);
        this.urls = new ArrayList();
        this.GMQLScriptUrl = " ";
    }

    
   /**
    * 
    * @param username
    * @param DataSetName
    * @param schemaDir
    * @param execType
    * @param urls 
    */

    public GMQLDataSet(String username, String DataSetName, String schemaDir, String execType, ArrayList<GMQLDataSetUrlField> urls) throws GMQLNotValidDatasetNameException {
        this.username = username;
        this.setDataSetName(DataSetName);
        this.schemaDir = schemaDir;
        this.setExecType(execType);
        this.urls = urls;
    }

    public GMQLDataSet(String username, String DataSetName, String schemaDir, String execType, ArrayList<GMQLDataSetUrlField> urls, String GMQLCodeUrl) throws GMQLNotValidDatasetNameException {
       this(username,DataSetName,schemaDir,execType,urls);
       this.GMQLScriptUrl = GMQLCodeUrl;
    }
    
    private void setDataSetName(String datasetname) throws GMQLNotValidDatasetNameException{
        if(datasetname.contains("$")||datasetname.contains("<")||datasetname.contains(">")
                ||datasetname.contains("#")||datasetname.contains("%")||datasetname.contains("!")
                ||datasetname.contains("*")||datasetname.contains("&")||datasetname.contains("^")
                ||datasetname.contains("+")||datasetname.contains("-")||datasetname.contains("=")
                ||datasetname.contains("\\")||datasetname.contains("/")
                ||datasetname.contains("[")||datasetname.contains("]"))
            throw new GMQLNotValidDatasetNameException("The DataSet Name is not Valid. Please do not use special Chars within DataSet name.");
        this.DataSetName = datasetname;
    }
    
    public GMQLDataSet(String username, String DataSetName, String schemaDir, String execType,String URL) throws GMQLNotValidDatasetNameException {
        this(username,DataSetName,schemaDir,execType,new ArrayList<GMQLDataSetUrlField>());
        
        int i = 0;
        if ((new File(URL)).isDirectory()) {
            File files[] = (new File(URL)).listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    if(name.endsWith(".meta"))return false;
                    File f = new File(dir.toString()+"/"+name+".meta");
                    logger.info(dir.toString() + "/" + name + " => has meta file - " + f.exists());
                    return f.exists();//(!name.endsWith(".meta")) && (!name.endsWith(".schema"));
                }
            });
 
            if(files.length == 0){
            logger.warn("The dataSet is empty.. \n\tCheck the files extensions. (i.e. sample.bed/sample.bed.meta)");
            }
            for (File file : files) {
                this.urls.add(new GMQLDataSetUrlField(file.getAbsolutePath(), (++i)+""));
            }
        } else {
            String url[] = URL.split(",");
            for (String u : url) {
                this.urls.add(new GMQLDataSetUrlField(u,(++i)+""));
            }
        }
    }
    
    public void copyDsXMLToHDFS() throws IOException{
        if (this.execType.equals(MAPREDUCE)) {
                utilities.copyfiletoHDFS(utilities.RepoDir + this.username + "/datasets/" + this.DataSetName + ".xml",
                        utilities.HDFSRepoDir + this.username + "/datasets/" + this.DataSetName + ".xml");
                
            }
    }
    
    public boolean Create(boolean CopySmaplestoDFS,boolean SamplesOnHDFS) {
        try {
            if(utilities.checkDSNameinRepo(this.username, DataSetName)) {
               logger.warn("This DataSet is already added to this user.");
                return false;
            }
            
            logger.info("Start Creating ( "+DataSetName + " )Dataset");
            createDataSetXML(CopySmaplestoDFS,SamplesOnHDFS);

            ////////////////////////// Build MetaData File//////////////////////////
            logger.info("Building the MetaData..");
            buildmetaDataWithIndex(SamplesOnHDFS);
            logger.info("Building the MetaData..\tDone.");
            
            ////////////////Copy the schema file to the user control folder/////////
            logger.info("Schema file has been copied ..");
            Path source, target;
            source = Paths.get(this.schemaDir);
            this.schemaDir = utilities.RepoDir + this.username + "/schema/" + DataSetName + ".schema";
            target = Paths.get(this.schemaDir);

            Files.copy(source, target, REPLACE_EXISTING);
            File f = new File(this.schemaDir);
            f.setExecutable(true,false);
            f.setReadable(true,false);
            f.setWritable(true,false);
            // if any Error happened ROLL BACK
            if(DScreateError){
                logger.warn("Some error occured :) Rolling back the operation...");
                this.Delete();
            }
        } catch (Exception pce) {
            logger.error(pce.getMessage(),pce);
            logger.error("Some error occured :) Rolling back the operation...");
            this.Delete();
        }
        return DScreateError;
    }
    
    private void createDataSetXML(boolean CopySmaplestoDFS, boolean samplesOnHDFS) throws IOException{
        
        logger.info("Copying data to HDFS /MAP REDUCE MODE/ ");
        // add url elements to dataset element  
        int i= 1;

        for (GMQLDataSetUrlField url : this.urls) {
//            if (this.execType.equals(MAPREDUCE)) {
//                Configuration conf = utilities.gethdfsConfiguration();
//                FileSystem fs = FileSystem.get(conf);
//
//                if (!samplesOnHDFS && (!(new File(url.geturl()).exists()))) {
//                    logger.error("Sample file is not found in local file system.. " + url);
//                    DScreateError = true;
//                } else if (samplesOnHDFS && (!fs.exists(new org.apache.hadoop.fs.Path(url.geturl())))) {
//                    logger.error("Sample file is not found in HDFS.. " + url.geturl());
//                    DScreateError = true;
//                }
//            }
            
            ////////////////////////////////////////////////////////////////////////////
            //copy the file to HDFS 
            ////////////////////////////////////////////////////////////////////////////
//            if (this.execType.equals(MAPREDUCE)) {
//                String hdfsdir = "";
//                if(!url.geturl().startsWith("hdfs"))
//                    hdfsdir = utilities.HDFSRepoDir + this.username + "/regions/" + url.geturl();
//                else
//                    hdfsdir = url.geturl();
//                if (CopySmaplestoDFS && (!samplesOnHDFS)) {
//                    utilities.copyfiletoHDFS(url.geturl(), hdfsdir);
//                } else if (CopySmaplestoDFS && samplesOnHDFS) {
//                    utilities.movefileInsideHDFS(url.geturl(),hdfsdir);
//                }
//            }
        }
        
        logger.info( "DataSet XML File is saved!");
    }
    
    private void buildmetaDataWithIndex(boolean samplesOnHDFS) {
        try {
            PrintWriter writer = new PrintWriter(
                    utilities.RepoDir + this.username + "/metadata/" + this.DataSetName + ".meta", "UTF-8");
            BufferedReader reader;
            String line;

            for (GMQLDataSetUrlField url : urls) {
                //////////////////////////Index meta Data
                //long id = util.Utilities.hash(HDFSRepoDir + USERNAME + "/regions/" + new File(url).getName());
                try {
                    File file = new File(url.geturl() + ".meta");

//                    org.apache.hadoop.fs.Path metaurl = new org.apache.hadoop.fs.Path(url.geturl() + ".meta");

//                    if (samplesOnHDFS && this.execType.equals(MAPREDUCE)) {
//                    if (Integer.parseInt(url.getID()) == 1) {
//                        LuceneIndex.buildIndex(utilities.RepoDir + this.username + "/indexes/" + this.DataSetName + "/",
//                                url.geturl() + ".meta", Integer.parseInt(url.getID()), true, true);
//                    } else {
//                        LuceneIndex.addSampletoIndex(utilities.RepoDir + this.username + "/indexes/" + this.DataSetName + "/",
//                                url.geturl() + ".meta", Integer.parseInt(url.getID()), true);
//                    }
//                        Configuration conf = utilities.gethdfsConfiguration();
//                        FileSystem fs = FileSystem.get(conf);
//
//                        if (fs.exists(metaurl)) {
//                            try {
//                                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(metaurl)));
//                                while ((line = bufferedReader.readLine()) != null) {
//                                    try {
//                                        writer.println(url.getID() + "\t" + line);
//                                    } catch (Exception e) {
//                                        e.printStackTrace();
//                                    }
//                                }
//                            } catch (Exception ex) {
//                                logger.error("Reading meta from HDFS and writing to local.");
//                            }
//                        } else {
//                            logger.error("File not found " + url.geturl());
//                        }
//                    } else
                    System.out.println(file.toPath()+"\t"+file.exists());
                        if (file.exists()) {
//                    if (Integer.parseInt(url.getID()) == 1) {
//                        LuceneIndex.buildIndex(utilities.RepoDir + this.username + "/indexes/" + this.DataSetName + "/",
//                                url.geturl() + ".meta", Integer.parseInt(url.getID()), true, false);
//                    } else {
//                        LuceneIndex.addSampletoIndex(utilities.RepoDir + this.username + "/indexes/" + this.DataSetName + "/",
//                                url.geturl() + ".meta", Integer.parseInt(url.getID()), false);
//                    }
                        reader = Files.newBufferedReader(file.toPath(), Charset.defaultCharset());
                        while ((line = reader.readLine()) != null) {
                            writer.println(url.getID() + "\t" + line);
//                          System.out.println(id + "\t" + line);
                        }
                        reader.close();
                    } else {
                        DScreateError = true;
                        logger.error("Meta file is not found .. " + url.geturl() + ".meta \tCheck the schema URL.. ");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            logger.info("Meta of" + DataSetName + " data set is Built... ");
            writer.close();
            File f = new File(utilities.RepoDir + this.username + "/metadata/" + this.DataSetName + ".meta");
            Utilities.setFullLocalPermissions(f);
            if (this.execType.equals(MAPREDUCE)) {
                utilities.copyfiletoHDFS(utilities.RepoDir + this.username + "/metadata/" +this.DataSetName + ".meta", 
                        utilities.HDFSRepoDir + this.username + "/metadata/" + this.DataSetName + ".meta");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
           logger.error("Some files are not in the proper format.. sample.bed / sample.bed.meta");
        }
    }
    
    public String getIndexURI(){return utilities.RepoDir + this.username + "/indexes/" + this.DataSetName;}

    public String Delete(){
        File index = new File(utilities.RepoDir + this.username + "/indexes/" + this.DataSetName);
        File meta = new File(utilities.RepoDir + this.username + "/metadata/" + this.DataSetName + ".meta");
        File dataset = new File(utilities.RepoDir + this.username + "/datasets/" + this.DataSetName + ".xml");
        File schema = new File(utilities.RepoDir + this.username + "/schema/" + this.DataSetName + ".schema");
        File regions = new File(utilities.RepoDir + this.username + "/regions/" + this.DataSetName );
        
        File result;
        try {
            result = new File(utilities.RepoDir + this.username + "/results/" + this.DataSetName.substring(0, this.DataSetName.lastIndexOf('_')));
            if (result.exists()) {
                Utilities.getInstance().deleteFromLocalFSRecursive(result.toString());
                logger.info(this.DataSetName + " results is deleted ..");
            } else {
                logger.warn(this.DataSetName + ", tmp results is not found in { results } folder..\n");
            }
        } catch (Exception ex) {
        }
        
        try {
            if (this.execType.equals(MAPREDUCE)) {
                Configuration conf = Utilities.getInstance().gethdfsConfiguration();
                utilities.deleteDFSDir(conf.get("fs.defaultFS")+utilities.HDFSRepoDir + this.username + "/datasets/" + this.DataSetName + ".xml");
                utilities.deleteDFSDir(conf.get("fs.defaultFS")+utilities.HDFSRepoDir + this.username + "/metadata/" + this.DataSetName + ".meta");
                try{
                utilities.deleteDFSDir(conf.get("fs.defaultFS")+utilities.HDFSRepoDir + this.username + "/results/" + this.DataSetName.substring(0,this.DataSetName.lastIndexOf('_')));
                }catch(Exception ex){
                    logger.info("No tmp result dir to delete.. ");
                }
                for (GMQLDataSetUrlField url : urls) {
                    if(!url.geturl().trim().startsWith("hdfs"))
                        utilities.deleteDFSSamples(conf.get("fs.defaultFS")+utilities.HDFSRepoDir + this.username + "/regions/" + url.geturl(), this.username);
                    else
                        utilities.deleteDFSSamples(url.geturl(), this.username);
                }
            }
            logger.info( this.DataSetName + " in HDFS samples are deleted ..");
        } catch (Exception ioe) {
            logger.error("Operation not allowed.. \n"
                            + "\tFiles are either already deleted or there is no permission to delete ... \n"
                            + "\tCheck you datasets using LIST ALL");
            logger.error(ioe.getMessage(),ioe);
        }
        
        if (dataset.exists()) {
            dataset.delete();
            logger.info(this.DataSetName + " dataset file deleted ..");
        } else {
            return this.DataSetName + ", dataset is not found..\n";
        }
        if (schema.exists()) {
            schema.delete();
            logger.info(
                    this.DataSetName + " Schema is deleted ..");
        } else {
            return this.DataSetName + ", schema is not found..\n";
        }
        
        if (regions.exists()) {
            Utilities.getInstance().deleteFromLocalFSRecursive(regions.toString());
            logger.info(this.DataSetName + " regions is deleted .. "+ regions.toString());
        } else {
            logger.warn(this.DataSetName + ", regions is not found in { regions } folder..\n");
        }
        
        if (meta.exists()) {
            meta.delete();
            logger.info(this.DataSetName + " meta Data is deleted ..");
        } else {
            logger.warn( this.DataSetName + ", meta is not found..\n");
        }
        if (index.exists()) {
            utilities.deleterecursive(index);
            index.delete();
            logger.info(this.DataSetName + " index is also deleted ..");
        } else {
            logger.warn( this.DataSetName + ", index is not found..\n");
        }

        logger.info("All files and folders related to " + this.DataSetName + " are now deleted\n"
                        + "\t except the user local original files..");

        return "\t Deleted...\n";
    }
    
     public boolean AddSampleToDS(String URL) throws IOException {
         int lastid = (urls.size()>0) ? Integer.parseInt(urls.get(urls.size() - 1).getID()) : 0;
         File meta = new File(URL+".meta");
         if(!meta.exists()){
             return false;
         }
         // Add new url to the list
         urls.add(new GMQLDataSetUrlField(URL, String.valueOf(lastid + 1)));
         if(this.execType.equals(MAPREDUCE))
         utilities.copyfiletoHDFS(URL, utilities.HDFSRepoDir + this.username + "/regions/" + Paths.get(URL).getParent()+"/"+Paths.get(URL).getFileName());
         logger.info("DONE...");

        //add sample metadata
        BufferedReader reader;
        String line;

        try (PrintWriter out = new PrintWriter(new BufferedWriter(
                new FileWriter(utilities.RepoDir + this.username + "/metadata/" + this.DataSetName + ".meta"
                        , true)))) {
            //long id = util.Utilities.hash(HDFSRepoDir+USERNAME+"/regions/"+new File(URL).getName());
            Path file = Paths.get(URL + ".meta");
            if (Files.exists(file) && Files.isReadable(file)) {
                //Index meta Data
                LuceneIndex.addSampletoIndex(utilities.RepoDir + this.username + "/indexes/" + this.DataSetName + "/",
                        URL + ".meta", lastid + 1, false);
                reader = Files.newBufferedReader(file, Charset.defaultCharset());
                while ((line = reader.readLine()) != null) {
                    out.println((lastid + 1) + "\t" + line);
                    logger.info((lastid + 1) + "\t" + line);
                }
                reader.close();
            }

        } catch (IOException e) {
            logger.error(e.getMessage(),e);
            //RollBack
            DelSampleFromDS(URL);
        }
        try {
            if(this.execType.equals(MAPREDUCE))
            utilities.copyfiletoHDFS(utilities.RepoDir + this.username + "/metadata/" + this.DataSetName + ".meta", utilities.HDFSRepoDir + this.username + "/metadata/" + this.DataSetName + ".meta");
        } catch (IOException ex) {
           logger.error( null, ex);
            //RollBack
            DelSampleFromDS(URL);
        }

        return true;
    }
     
     public int DelSampleFromDS(String URL) {
        //Delete the Sample From the DataSet XML
         int deletedid = checkSampleInDataSet(URL, true);
        if (deletedid != 0) {
            try {
                //Delete the Sample From HDFS
                if (this.execType.equals(MAPREDUCE)) {
                    if(!URL.startsWith("hdfs"))
                        utilities.deleteDFSDir(utilities.HDFSRepoDir + this.username + "/regions/" + URL);
                    else
                        utilities.deleteDFSDir( URL);
                }
                
                //Delete the Sample From the MetaData & Index and Sync with HDFS
                PrintWriter writer = new PrintWriter(utilities.RepoDir + this.username + "/metadata/" + this.DataSetName + ".meta.tmp", "UTF-8");
                BufferedReader reader;
                String line;
                Path file = Paths.get(utilities.RepoDir + this.username + "/metadata/" + this.DataSetName + ".meta");
                if (Files.exists(file) && Files.isReadable(file)) {
                    LuceneIndex.deleteIndexedSamplebyURL(utilities.RepoDir + this.username + "/indexes/" + this.DataSetName + "/", URL + ".meta");
                    reader = Files.newBufferedReader(file, Charset.defaultCharset());
                    while ((line = reader.readLine()) != null) {
                        String str[] = line.split("\t");
                        if (!(Integer.parseInt(str[0]) == deletedid)) {
                            writer.println(line);
                        }
                    }
                    reader.close();
                } else {
                    logger.error("This is a folder");
                }
                writer.close();
                File f1 = new File(utilities.RepoDir + this.username + "/metadata/" + this.DataSetName + ".meta");
                File f = new File(utilities.RepoDir + this.username + "/metadata/" + this.DataSetName + ".meta.tmp");
                f.renameTo(f1);

                if (this.execType.equals(MAPREDUCE)) {
                    utilities.copyfiletoHDFS(utilities.RepoDir + this.username + "/metadata/" + this.DataSetName + ".meta",
                            utilities.HDFSRepoDir + this.username + "/metadata/" + this.DataSetName + ".meta");
                }

            } catch (Exception ex) {
                logger.error( ex.getMessage(), ex);
            }
        } else {
            logger.warn("The Sample does not exists in this data set. Please check the Sample name ");
        }
        return deletedid;
    }
     
     public int checkSampleInDataSet(String URL, boolean delete) {
        boolean found = false;
        int delid = 0;
        int i=0;
        try {
            for (GMQLDataSetUrlField url: this.urls) {
                if (url.geturl().trim().equals(URL.trim())) {
                    delid = Integer.parseInt(url.getID());
                    found = true;
                    break;
                }
                i++;
            }
            if (delete&&found) {
                System.out.println("Deleted url = " + urls.get(i).geturl());
                urls.remove(i);        
            }
        } catch (NumberFormatException pce) {
            logger.error(pce.getMessage(),pce);
        }
        if (!found) {
            logger.warn("The sample is not found in the dataset.. ");
        }
        else {
            logger.info ("The sample ( "+URL+" ) is located in the dataset.");
            if(delete) logger.info("The sample is deleted in the dataset.");
        }
        return delid;
    }
     
     
     public ArrayList<String> ListDSSamples() {
        ArrayList<String> samples = null;
        try {
            
            if (!utilities.checkDSName(this.username, this.DataSetName)) {
                logger.warn("\nWARN: The dataset " + this.DataSetName + " is not found. \n\tPlease check the name again.");
                return samples;
            }
            
            samples = new ArrayList<>();
            for (GMQLDataSetUrlField url : urls) {
                samples.add(url.geturl());
                System.out.println(url.geturl());
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return samples;
    }
     
     public void CopyToLocal(String LocaldirURL) {
        Map<Integer, String> samples;
        samples = new HashMap<>();
        File f = new File(LocaldirURL);
         try {
            if (!utilities.checkDSNameinRepo(this.username, this.DataSetName)) {
                logger.error(" The dataset " + this.DataSetName + " is not found. \n\tPlease check the name again.");
                return;
            }
        logger.info("Start ...");
            
            if (!f.exists()) {
                f.mkdir();
                Utilities.setFullLocalPermissions(f);
                logger.info("Destination folder is created ...");
            }
            if (!f.isDirectory()) {
                logger.error("Please specify an absulot path to a directory..");
                return;
            }
              
            
            for (GMQLDataSetUrlField url : urls) {
                samples.put(Integer.parseInt(url.getID()), url.geturl());
                if (this.execType.equals(MAPREDUCE)) {
                    logger.info(url.geturl());
                    if(!url.geturl().startsWith("hdfs"))
                        utilities.copyfiletoLocal(utilities.HDFSRepoDir + username + "/regions/" + url.geturl(),
                            LocaldirURL);
                    else
                        utilities.copyfiletoLocal(url.geturl(), LocaldirURL);
                } else {
                    File sample = new File(url.geturl());
                    File dist = new File(LocaldirURL+"/"+sample.getName());
                    FileUtils.copyFile(sample,dist);
                }
                
                //Logger.getLogger(RepositoryManager.class.getName()).log(Level.FINE, node.getTextContent());
            }
            
           
             if (!GMQLScriptUrl.trim().equals("")) {
                 Path GMQLScript = Paths.get(GMQLScriptUrl);
                 Files.copy(GMQLScript, Paths.get(LocaldirURL + "/" + GMQLScript.getFileName()));
             }
             
             if (!schemaDir.trim().equals("")) {
                 Path schema = Paths.get(schemaDir);
                 Files.copy(schema, Paths.get(LocaldirURL + "/" + schema.getFileName()));
             }
            
            //--------------------------meta data cut up--------------------------------
            BufferedReader reader = new BufferedReader(
                    new FileReader(utilities.RepoDir + username + "/metadata/" + this.DataSetName + ".meta"));
            String row = reader.readLine();
            String[] Arow = row.split("\t");
            int buffer = Integer.parseInt(Arow[0]);

            PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(
                    LocaldirURL + "/" + Paths.get(samples.get(buffer)).getFileName().
                    toString() + ".meta")));
            out.println(Arow[1] + "\t" + Arow[2]);
            int i = 0;
            while ((row = reader.readLine()) != null) {
                Arow = row.split("\t");
                if (Integer.parseInt(Arow[0]) != buffer) {
                    buffer = Integer.parseInt(Arow[0]);
                    out.close();
                    i++;
                    out = new PrintWriter(new BufferedWriter(new FileWriter(
                            LocaldirURL + "/" + Paths.get(samples.get(buffer)).getFileName().
                            toString() + ".meta")));

                }
                out.println(Arow[1] + "\t" + Arow[2]);
            }
            out.close();
            logger.info("The copying is done to: " + LocaldirURL);
        } catch (Exception ex) {
            f.delete();
            logger.error(ex.getMessage(),ex);
        }
     }
     
    /**
     * @return the fields
     */
    public List<GMQLDataSetUrlField> getURLs() {
        if (urls == null) {
            return new ArrayList<>();
        }
        return urls;
    }

    /**
     * @param URLs
     */
    public void setURLs(ArrayList<GMQLDataSetUrlField> URLs) {
        this.urls = URLs;
    }
    
    public String getSchema() {
        return schemaDir;
    }

    public void setSchema(String SchemaDir) {
        this.schemaDir = SchemaDir;
    }
    
    public void setGMQLCodeUrl(String GMQLCodeUrl) {
        this.GMQLScriptUrl = GMQLCodeUrl;
    }
    
    public String getGMQLCodeUrl(String GMQLCodeUrl) {
        return this.GMQLScriptUrl;
    }
    
    public String getExecType() {
        return this.execType;
    }

     public void setName(String name) {
        this.DataSetName = name;
    }
    
    public String getName() {
        return this.DataSetName;
    }
    
     public void setuserName(String name) {
        this.username = name;
    }
    
    public String getUserName() {
        return this.username;
    }
    /**
     * @param execType
     */
    public final void setExecType(String execType) {
        execType = execType.toUpperCase();
        if(execType.equals("LOCAL") || execType.equals("MAPREDUCE")){
            this.execType = execType;
        }
        else {
            this.execType = "LOCAL";
            logger.warn("The execution type was entered Wrong ("+execType
                    +") so we use the default : LOCAL");
        }
    }
    
}
