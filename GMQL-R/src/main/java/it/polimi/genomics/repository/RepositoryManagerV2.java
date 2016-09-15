package it.polimi.genomics.repository;

/**
 * Created by Abdulrahman Kaitoua on 09/09/15.
 * Email: abdulrahman.kaitoua@polimi.it
 */

import it.polimi.genomics.repository.datasets.GMQLDataSet;
import it.polimi.genomics.repository.datasets.GMQLDataSetCollection;
import it.polimi.genomics.repository.datasets.GMQLDataSetUrlField;
import it.polimi.genomics.repository.datasets.GMQLNotValidDatasetNameException;
import it.polimi.genomics.repository.util.Utilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 * @author abdulrahman kaitoua <abdulrahman dot kaitoua at polimi dot it>
 */
public class RepositoryManagerV2 {

    private static final Logger logger = LoggerFactory.getLogger(RepositoryManagerV2.class);
    private String username;
    private Utilities u = Utilities.getInstance();

    /**
     *
     * @param username
     */
    public RepositoryManagerV2(String username) {
        this.username = username;
    }

    /**
     *
     * @param username
     * @return
     */
    public boolean registerUser(String username) {
        if (u.MODE.equals("MAPREDUCE")) {
            try {
                System.out.println("INFO: HDFS Folders Creation ...");
                System.out.println("INFO: HDFS Folder, " + u.HDFSRepoDir + username + "/regions/"
                        + createDFSDir(u.HDFSRepoDir + username + "/regions/"));
                System.out.println("INFO: HDFS Folder, " + u.HDFSRepoDir + username + "/metadata/"
                        + createDFSDir(u.HDFSRepoDir + username + "/metadata/"));
                System.out.println("INFO: HDFS Folder, " + u.HDFSRepoDir + username + "/results/"
                        + createDFSDir(u.HDFSRepoDir + username + "/results/"));
                System.out.println("INFO: HDFS Folder, " + u.HDFSRepoDir + username + "/results/"
                        + createDFSDir(u.HDFSRepoDir + username + "/datasets/"));

            } catch (IOException ioe) {
                logger.error(ioe.getMessage(), ioe);
            }
        }
        System.out.println("INFO:  Local Folders Creation ...");
        File indexes = new File(u.RepoDir + username + "/indexes");
        File datasets = new File(u.RepoDir + username + "/datasets");
        File metadata = new File(u.RepoDir + username + "/metadata");
        File schema = new File(u.RepoDir + username + "/schema");
        File results = new File(u.RepoDir + username + "/results");
        File regions = new File(u.RepoDir + username + "/regions");
        File queries = new File(u.RepoDir + username + "/queries");
        try {
            System.out.println("INFO:  Folder, " + u.RepoDir + username + "/indexes/\t" + (indexes.mkdirs() ? "Done" : "failed"));
            System.out.println("INFO:  Folder, " + u.RepoDir + username + "/datasets/\t" + (datasets.mkdir() ? "Done" : "failed"));
            System.out.println("INFO:  Folder, " + u.RepoDir + username + "/metadata/\t" + (metadata.mkdir() ? "Done" : "failed"));
            System.out.println("INFO:  Folder, " + u.RepoDir + username + "/schema/\t" + (schema.mkdir() ? "Done" : "failed"));
            System.out.println("INFO:  Folder, " + u.RepoDir + username + "/results/\t" + (results.mkdir() ? "Done" : "failed"));
            System.out.println("INFO:  Folder, " + u.RepoDir + username + "/regions/\t" + (regions.mkdir() ? "Done" : "failed"));
            System.out.println("INFO:  Folder, " + u.RepoDir + username + "/queries/\t" + (queries.mkdir() ? "Done" : "failed"));
            Utilities.setFullLocalPermissions(indexes);
            Utilities.setFullLocalPermissions(datasets);
            Utilities.setFullLocalPermissions(metadata);
            Utilities.setFullLocalPermissions(schema);
            Utilities.setFullLocalPermissions(results);
            Utilities.setFullLocalPermissions(regions);
            Utilities.setFullLocalPermissions(queries);
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            return false;
        }
        return true;
    }

    /**
     *
     * @param username
     */
    public void unregisterUser(String username) {
        if (u.MODE.equals("MAPREDUCE")) {
            try {
                System.out.println("INFO: HDFS Folders Deletion ...");
                System.out.println("INFO: HDFS Folder, " + u.HDFSRepoDir + username + "/datasets/\t Status:"
                        + (u.deleteDFSDir(u.HDFSRepoDir + username + "/datasets/") ? "Done." : "Error"));
                System.out.println("INFO: HDFS Folder, " + u.HDFSRepoDir + username + "/regions/\t Status:"
                        + (u.deleteDFSDir(u.HDFSRepoDir + username + "/regions/") ? "Done." : "Error"));
                System.out.println("INFO: HDFS Folder, " + u.HDFSRepoDir + username + "/metadata/\t Status:"
                        + (u.deleteDFSDir(u.HDFSRepoDir + username + "/metadata/") ? "Done." : "Error"));
                System.out.println("INFO: HDFS Folder, " + u.HDFSRepoDir + username + "/results/\t Status:"
                        + (u.deleteDFSDir(u.HDFSRepoDir + username + "/results/") ? "Done." : "Error"));

            } catch (IOException ioe) {
                logger.error(ioe.getMessage(), ioe);
            }
        }
        System.out.println("INFO:  Local Folders Deletion ..." + u.RepoDir + username + "/");
        u.deleterecursive(new File(u.RepoDir + username + "/"));
        (new File(u.RepoDir + username + "/")).delete();
        //(new File(RepoDir+Username+"/")).mkdirs();
    }

    /**
     *
     * @param username
     * @param DSName
     * @param SchemaDir
     * @param URL
     * @return
     */
    public String CreateDS(String username, String DSName, String SchemaDir, String execType, String URL) throws GMQLNotValidDatasetNameException {

        if (!(new File(SchemaDir).exists() && new File(SchemaDir).isFile())) {
            logger.error(
                    " Schema file is not found .. \n\t Check the schema URL.. ");
            return "ERROR: Schema file Not Found..";
        }

        GMQLDataSetCollection DataSetCol = new GMQLDataSetCollection();
        GMQLDataSet dataSet = new GMQLDataSet(username, DSName, SchemaDir, execType, URL);
        boolean copytolocal;
        if (execType.toUpperCase().equals("MAPREDUCE")) {
            copytolocal = true;
        } else {
            copytolocal = false;
        }
        dataSet.Create(copytolocal, false);
        DataSetCol.addDataSetToCollection(dataSet);

        try {
            DataSetCol.bindGMQLDataSetCollection(Paths.get(u.RepoDir + username + "/datasets/" + DSName + ".xml"), DataSetCol);
            File f = new File(u.RepoDir + username + "/datasets/" + DSName + ".xml");
            f.setExecutable(true, false);
            f.setReadable(true, false);
            f.setWritable(true, false);
            dataSet.copyDsXMLToHDFS();
        } catch (JAXBException | IOException ex) {
            logger.warn(ex.getMessage(), ex);
            return "Error: DataSet is not created..";
        }
        return "INFO: DataSet Created  ";
    }

    public String CreateDS(String username, String DSName, String SchemaDir, String execType, List<String> urls, String GMQLCodeUrl, boolean CopySmaplestoDFS, boolean SamplesOnHDFS) throws GMQLNotValidDatasetNameException {

        if (!(new File(SchemaDir).exists() && new File(SchemaDir).isFile())) {
            logger.error(
                    " Schema file is not found .. \n\t Check the schema URL.. ");
            return "ERROR: Schema file Not Found..";
        }

        GMQLDataSetCollection DataSetCol = new GMQLDataSetCollection();
        ArrayList<GMQLDataSetUrlField> urlsFields = new ArrayList<>();
        int i = 1;
        for (String url : urls) {
            urlsFields.add(new GMQLDataSetUrlField(url, i + ""));
            i++;
        }
        GMQLDataSet dataSet = new GMQLDataSet(username, DSName, SchemaDir, execType, urlsFields, GMQLCodeUrl);
        dataSet.Create(CopySmaplestoDFS, SamplesOnHDFS);
        DataSetCol.addDataSetToCollection(dataSet);
        try {
            DataSetCol.bindGMQLDataSetCollection(Paths.get(u.RepoDir + username + "/datasets/" + DSName + ".xml"), DataSetCol);
            File f = new File(u.RepoDir + username + "/datasets/" + DSName + ".xml");
            f.setExecutable(true, false);
            f.setReadable(true, false);
            f.setWritable(true, false);
            dataSet.copyDsXMLToHDFS();
        } catch (JAXBException | IOException ex) {
            logger.warn(ex.getMessage(), ex);
            return "Error: DataSet is not created..";
        }
        return "INFO: DataSet Created  ";
    }

    private String createDFSDir(String url) throws IOException {
        Configuration conf = new Configuration();

        conf.addResource(
                new org.apache.hadoop.fs.Path(u.CoreConfigurationFiles));
        conf.addResource(
                new org.apache.hadoop.fs.Path(u.HDFSConfigurationFiles));
//
//        System.out.println("u.CoreConfigurationFiles = " + u.CoreConfigurationFiles);
//        System.out.println("url = " + url);
//        System.out.println("u.HDFSConfigurationFiles = " + u.HDFSConfigurationFiles);
//        System.out.println("FS: "+conf.get("fs.defaultFS"));
//        System.out.println("old: "+ conf.get("fs.default.name"));
//        System.out.println(url);
        conf.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        conf.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );
        FileSystem fs = FileSystem.get(conf);
        if (!fs.mkdirs(new org.apache.hadoop.fs.Path(url))) {
            return "Failed";
        }
        fs.setPermission(new org.apache.hadoop.fs.Path(url), Utilities.permission);
        return "Done";
    }

    /**
     *
     * @param DSName
     * @param username
     * @return
     * @throws IOException
     */
    public boolean DeleteDS(String DSName, String username) throws IOException {

        GMQLDataSetCollection GMQLDSCol = new GMQLDataSetCollection();
        GMQLDataSet dataSet = new GMQLDataSet();
        try {
            System.out.println(u.RepoDir + username + "/datasets/" + DSName + ".xml");

            GMQLDSCol = GMQLDSCol.parseGMQLDataSetCollection(Paths.get(u.RepoDir + username + "/datasets/" + DSName + ".xml"));
            dataSet = GMQLDSCol.getDataSetList().get(0);
            if (!dataSet.getURLs().isEmpty()) {
                System.out.println(dataSet.getExecType() + " \t" + dataSet.getURLs().get(0).geturl());
            }
            dataSet.Delete();

        } catch (JAXBException | FileNotFoundException ex) {
            logger.error(ex.getMessage(), ex);
            return false;
        }
        System.out.println("INFO: \tDeleted..\n");
        return true;
    }

    /**
     *
     * @param DSName
     * @param URL
     * @param username
     * @return
     */
    public boolean AddSampleToDS(String DSName, String URL, String username) {
        GMQLDataSetCollection GMQLDSCol = new GMQLDataSetCollection();
        GMQLDataSet dataSet = new GMQLDataSet();
        try {
            GMQLDSCol = GMQLDSCol.parseGMQLDataSetCollection(Paths.get(u.RepoDir + username + "/datasets/" + DSName + ".xml"));
            dataSet = GMQLDSCol.getDataSetList().get(0);
            dataSet.setuserName(username);
            if (!dataSet.AddSampleToDS(URL)) {
                return false;
            }
            GMQLDSCol.clearDataSetList();
            GMQLDSCol.addDataSetToCollection(dataSet);
            GMQLDSCol.bindGMQLDataSetCollection(Paths.get(u.RepoDir + username + "/datasets/" + DSName + ".xml"), GMQLDSCol);
            dataSet.copyDsXMLToHDFS();
            // u.copyfiletoHDFS(u.RepoDir + username + "/datasets/" + DSName + ".xml", u.HDFSRepoDir + username + "/datasets/" + DSName + ".xml");

        } catch (JAXBException | IOException ex) {
            logger.error(ex.getMessage(), ex);
            return false;
        }
        return true;
    }

    /**
     *
     * @param DSName
     * @param URL
     * @param username
     * @return
     */
    public boolean DelSampleFromDS(String DSName, String URL, String username) {
        GMQLDataSetCollection GMQLDSCol = new GMQLDataSetCollection();
        GMQLDataSet dataSet = new GMQLDataSet();
        boolean success = false;
        try {
            GMQLDSCol = GMQLDSCol.parseGMQLDataSetCollection(Paths.get(u.RepoDir + username + "/datasets/" + DSName + ".xml"));
            dataSet = GMQLDSCol.getDataSetList().get(0);
            dataSet.setuserName(username);
            System.out.println("URL = " + URL);
            if (dataSet.DelSampleFromDS(URL) != 0) {
                success = true;
            }
            GMQLDSCol.clearDataSetList();
            GMQLDSCol.addDataSetToCollection(dataSet);
            GMQLDSCol.bindGMQLDataSetCollection(Paths.get(u.RepoDir + username + "/datasets/" + DSName + ".xml"), GMQLDSCol);
            if (success) {
                dataSet.copyDsXMLToHDFS();
                //u.copyfiletoHDFS(u.RepoDir + username + "/datasets/" + DSName + ".xml", u.HDFSRepoDir + username + "/datasets/" + DSName + ".xml");
            }
        } catch (JAXBException | IOException ex) {
            logger.error(ex.getMessage(), ex);
            return false;
        }

        return success;
    }

    /**
     *
     * @return
     */
    public ArrayList<String> ListAllDataSets(String username) {
        ArrayList<String> datasets = null;
        File userdatasetdir = new File(u.RepoDir + username + "/datasets/");
        if (!userdatasetdir.exists()) {
            logger.error("There is no user with the name { " + username + " }");
            return datasets;
        }
        File[] files = userdatasetdir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".xml");
            }
        });
        if (files == null) {
            logger.info("INFO: \tThere is no Datasets to show... \n \tCreate Data set though \"createDS\"");
        }
        datasets = new ArrayList<>();
        System.out.println("INFO: \t" + username + " DataSets: " + files.length);
        String[] userDatasets = new String[files.length];
        int i = 0;
        for (File xmlfile : files) {
            userDatasets[i] = xmlfile.getName().subSequence(0, xmlfile.getName().length() - 4).toString();
            datasets.add(userDatasets[i]);
            i++;
        }
        Arrays.sort(userDatasets);

        for (int j = 0; j < userDatasets.length; j++) {
            System.out.println("\t" + userDatasets[j]);
        }

        if (this.username.equals("public")) {
            return datasets;
        }

        File publicdatasetdir = new File(u.RepoDir + "public" + "/datasets/");
        if (!publicdatasetdir.exists()) {
            logger.error("There is no user with the name { public }");
            return datasets;
        }
        File[] publicfiles = publicdatasetdir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".xml");
            }
        });

        String[] publicDatasets = new String[publicfiles.length];
        System.out.println("\nINFO: \tPublic DataSets:");
        i=0;
        for (File xmlfile : publicfiles) {
            publicDatasets[i] =xmlfile.getName().subSequence(0, xmlfile.getName().length() - 4).toString();
            datasets.add("public." + publicDatasets[i]);
            i++;
        }
        Arrays.sort(publicDatasets);
        for (int j = 0; j < publicDatasets.length; j++) {
            System.out.println("\tpublic." +publicDatasets[j]);
        }

        return datasets;
    }

    /**
     *
     * @param DSName
     * @return
     */
    public ArrayList<String> ListDSSamples(String DSName, String username) {
        GMQLDataSetCollection GMQLDSCol = new GMQLDataSetCollection();
        GMQLDataSet dataSet = new GMQLDataSet();
        ArrayList<String> samples;
        if (DSName.contains("public.")) {
            logger.info("This is public data set...");
            username = "public";
            DSName = DSName.substring("public.".length(), DSName.length());
            return ListDSSamples(DSName, username);
        }
        try {
            GMQLDSCol = GMQLDSCol.parseGMQLDataSetCollection(Paths.get(u.RepoDir + username + "/datasets/" + DSName + ".xml"));
            dataSet = GMQLDSCol.getDataSetList().get(0);
            dataSet.setuserName(username);
            samples = dataSet.ListDSSamples();

        } catch (JAXBException ex) {
            logger.error(ex.getMessage(), ex);
            return null;
        } catch (FileNotFoundException ex) {
            logger.error("There is no DataSet with this name (" + DSName + ") Please check the name.");
            return null;
        }
        return samples;
    }

    /**
     *
     * @param DSName
     * @param LocaldirURL
     */
    public void CopyDSSamplesToLocal(String DSName, String LocaldirURL, String username) {
        GMQLDataSetCollection GMQLDSCol = new GMQLDataSetCollection();
        GMQLDataSet dataSet = new GMQLDataSet();

        if (DSName.contains("public.")) {
            username = "public";
            logger.info("This is public data set...");
            DSName = DSName.substring("public.".length(), DSName.length());
            CopyDSSamplesToLocal(DSName, LocaldirURL, username);
            return;
        }
        try {
            GMQLDSCol = GMQLDSCol.parseGMQLDataSetCollection(Paths.get(u.RepoDir + username + "/datasets/" + DSName + ".xml"));
            dataSet = GMQLDSCol.getDataSetList().get(0);
            dataSet.setuserName(username);
            dataSet.CopyToLocal(LocaldirURL);

        } catch (JAXBException | FileNotFoundException ex) {
            logger.error(ex.getMessage(), ex);
        }

    }

    /**
     *
     * @param DSName
     * @param URL
     * @param delete
     * @return
     */
    public int checkSampleIndataset(String DSName, String URL, boolean delete) {
        boolean found = false;
        int delid = 0;
        try {

            File fXmlFile = new File(u.RepoDir + this.username + "/datasets/" + DSName + ".xml");
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(fXmlFile);
            doc.getDocumentElement().normalize();

            Node n = doc.getElementsByTagName("dataset").item(0);

            // search for the url
            NodeList list = n.getChildNodes();
            for (int i = 0; i < list.getLength(); i++) {

                Node node = list.item(i);
                if (node.getTextContent().equals(URL)) {
                    if (delete) {
                        n.removeChild(node);
                    }
                    delid = Integer.parseInt(node.getAttributes().getNamedItem("id").getNodeValue());
                    found = true;
                    // Logger.getLogger(RepositoryManager.class.getName()).log(Level.FINE, node.getTextContent());
                }
            }

            if (delete && found) {
                // write the content into xml file
                TransformerFactory transformerFactory = TransformerFactory.newInstance();
                Transformer transformer = transformerFactory.newTransformer();
                DOMSource source = new DOMSource(doc);
                StreamResult result = new StreamResult(new File(u.RepoDir + this.username + "/datasets/" + DSName + ".xml"));
                transformer.transform(source, result);
                if (u.MODE.equals("MAPREDUCE")) {
                    u.copyfiletoHDFS(u.RepoDir + this.username + "/datasets/" + DSName + ".xml", u.HDFSRepoDir + this.username + "/datasets/" + DSName + ".xml");
                }
                logger.info("Done");
            }
        } catch (IOException | TransformerException | SAXException | ParserConfigurationException pce) {
            logger.error(pce.getMessage(), pce);
        }
        if (!found) {
            logger.warn("The sample is not found in the dataset.. ");
        }
        return delid;
    }

    /**
     *
     * @param args
     * @throws IOException
     */
    public static void main(String args[]) throws IOException {
        BasicConfigurator.configure();
        //System.out.println("The number of arguments"+args.length+"\t"+args[0]);
        String usage = "\t.........................................................\n"
                + "\t.........................................................\n"
                + "\t...........GMQL Repository Manager help..................\n"
                + "\t.........................................................\n"
                + "\t.........................................................\n\n"
                + "\tRepositoryManagerV2 $COMMAND\n\n"
                + "\tAllowed Commands are :\n\n"
                + "\tRegisterUser\n\n"
                + "\t\tTo register the current user to the GMQL repository.\n"
                + "\tUnRegisterUser\n\n"
                + "\t\tTo unregister the current user from GMQL repository.\n"
                + "\t\tAll the datasets, history, User private schemata and user results will be deleted.\n"
                + "\t\tThe Samples on the Local File System will not be deleted.\n"
                + "\tCREATEDS DS_NAME SCHEMA_URL SAMPLES_PATH \n"
                + "\t\tDS_NAME is the new dataset name\n\n"
                + "\t\tSCHEMA_URL is the path to the schema file\n"
                + "\t\t - The schema can be a keyword { BED | BEDGRAPH | NARROWPEAK | BROADPEAK }\n"
                + "\t\tSAMPLES_PATH is the path to the samples, can be one of the following formats:\n"
                + "\t\t - Samples urls separated by a comma with no spaces in between:\n"
                + "\t\t   /to/the/path/sample1.bed,/to/the/path/sample2.bed \n"
                + "\t\t - Samples folder Path: /to/the/path/samplesFolder/ \n"
                + "\t\t   in this case all the samples with an associated metadata will be added to the dataset.\n"
                + "\t\tTIP: each sample file must have a metadata file with the same full name and aditional .meta extension \n"
                + "\t\t     for example: { sample.bed sample.bed.meta }\n\n"
                + "\tDELETEDS DS_NAME \n"
                + "\t\t To Delete a dataset named DS_NAME\n\n"
                + "\tADDSAMPLE DS_NAME SAMPLE_URL \n"
                + "\t\tDS_NAME the dataset name (It has to be already added in the system). \n"
                + "\t\tSAMPLE_URL is the path to the sample. No need to add the metadata Path since it must be in the same folder.\n"
                + "\t\t           For example: /to/the/path/sample.bed\n\n"
                + "\tDELETESAMPLE DS_NAME SAMPLE_URL \n"
                + "\t\tDelete one sample form the dataset named DS_NAME \n"
                + "\t\tSAMPLE_URL must be identical to what { LIST DS_NAME } command prints. \n\n"
                + "\tLIST ALL|DS_NAME\n"
                + "\t\tALL to print all the datasets for the current user and the public user. \n"
                + "\t\tDS_NAME to print the samples of this dataset\n\n"
                + "\tCopyDSToLocal DS_NAME LOCAL_DIRECTORY \n"
                + "\t\tThis command copy all the samples of DS_NAME to local folder.\n"
                + "\t\tThe samples will be copied with its metadata.\n"
                + "\t\tLOCAL_DIRECTORY is the full path to the local location. \n\n"
                + "INFO: For more information read the GMQL shell commands document.\n\n";

        if (args.length > 0 && ("h".equals(args[0]) || "help".equals(args[0]))) {
            System.out.println(usage);
            System.exit(0);
        }

        if (args.length == 0) {
            System.out.println("WARN:\tThe specified command is not supported.. \n"
                    + "\tType { RepositoryManagerV2 h | help } for help...");
            return;
        }

        //RepoDir = "~/GMQLData/";
        String username = System.getProperty("user.name");

        RepositoryManagerV2 mr = new RepositoryManagerV2(username);

        Utilities.USERNAME = username;

        String Command = args[0];
        switch (Command.toLowerCase()) {
            case "registeruser":
                if (args.length > 1) {
                    username = args[1];
                    mr.username = username;
                    Utilities.USERNAME = username;
                }
                if (mr.registerUser(username)) {
                    System.out.println("INFO: \tFolders are created \n\n");
                } else {
                    System.out.println("ERROR: \tFolders are either created before or you do not have a permission..");
                }
                break;
            case "unregisteruser":
                if (args.length > 1) {
                    username = args[1];
                    mr.username = username;
                    Utilities.USERNAME = username;
                }

                mr.unregisterUser(username);

                break;
            case "createds":
                if (args.length < 4) {
                    System.out.println(usage);
                    break;
                } else if (args.length > 4) {
                    username = args[4];
                    mr.username = username;
                    Utilities.USERNAME = username;
                }
                File confDir = new File(Utilities.getInstance().GMQLHOME + "/conf/");
                File[] schema = confDir.listFiles(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        return name.toLowerCase().endsWith(".schema");
                    }
                });
/*
                String schemaType;
                for (File file : schema) {
                    schemaType = file.getName().split("\\.")[0];
                    logger.info("You can choose from the following defined schemaType = " + schemaType);
                    if (schemaType.toLowerCase().equals(args[2].toLowerCase())) {
                        args[2] = file.toString();
                    }
                }
                logger.info("Schema File = " + args[2]);
*/
                try {
                    mr.CreateDS(username, args[1], args[2], Utilities.getInstance().MODE.toString(), args[3]);
                } catch (GMQLNotValidDatasetNameException ex) {

                }

                break;
            case "deleteds":
                if (args.length < 2) {
                    System.out.println(usage);
                    break;
                } else if (args.length > 2) {
                    username = args[2];
                    mr.username = username;
                    Utilities.USERNAME = username;
                }
                System.out.println(args[1]);
                if (args[1].endsWith("!")) {
                    final String datasetname = args[1].substring(0, args[1].length() - 1);
                    System.out.println("Delete all the datasets that starts with " + datasetname);

                    File dataSetDir = new File(Utilities.getInstance().RepoDir + username + "/datasets/");
                    File datasets[] = dataSetDir.listFiles(new FilenameFilter() {
                        @Override
                        public boolean accept(File dir, String name) {
                            return name.matches(datasetname + "*.*\\.xml");//(!name.endsWith(".meta")) && (!name.endsWith(".schema"));
                        }
                    });
                    for (int i = 0; i < datasets.length; i++) {
                        System.out.println("datasets[i] = " + datasets[i].getName().split("\\.")[0]);
                    }
                    Console console = System.console();
                    String input = console.readLine("Are you sure you want to delete all (Y|N):");
                    if (input.toLowerCase().equals("y")) {
                        for (int i = 0; i < datasets.length; i++) {
                            mr.DeleteDS(datasets[i].getName().split("\\.")[0], username);
                        }
                    } else {
                        return;
                    }
                } else {
                    mr.DeleteDS(args[1], username);
                }
                break;
            case "addsample":
                if (args.length < 3) {
                    System.out.println(usage);
                    break;
                } else if (args.length > 3) {
                    username = args[3];
                    mr.username = username;
                    Utilities.USERNAME = username;
                }
                mr.AddSampleToDS(args[1], args[2], username);
                break;
            case "deletesample":
                if (args.length < 3) {
                    System.out.println(usage);
                    break;
                } else if (args.length > 3) {
                    username = args[3];
                    mr.username = username;
                    Utilities.USERNAME = username;
                }
                mr.DelSampleFromDS(args[1], args[2], username);
                break;
            case "list":
                if (args.length < 2) {
                    System.out.println(usage);
                    break;
                } else if (args.length > 2) {
                    username = args[2];
                    System.out.println("username = " + username);
                    mr.username = username;
                    Utilities.USERNAME = username;
                }
                if ("all".equals(args[1].toLowerCase())) {
                    mr.ListAllDataSets(username);
                } else {
                    mr.ListDSSamples(args[1], username);
                }
                break;
            case "copydstolocal":
                String DatasetName = "",
                        Locallocatoin = "";
                if (args.length < 3 || args.length > 4) {
                    System.out.println(usage);
                    break;
                }

                DatasetName = args[1];
                Locallocatoin = args[2];
                if (args.length == 4) {
                    username = args[3];
                }
                mr.username = username;
                Utilities.USERNAME = username;
                mr.CopyDSSamplesToLocal(DatasetName, Locallocatoin, username);

                break;
            default:
                System.out.println("ERROR: \tThe Command is not defined....");
                System.out.println(usage);
                break;
        }

        //mr.CreateUserFolders();
        //mr.CreateDS("MyBEB", "/home/gql_repository/data/user_3/schema", "/home/abdulrahman/splitted_files/ANN/1.txt,/home/abdulrahman/splitted_files/ANN/2.txt,/home/abdulrahman/splitted_files/ANN/3.txt,/home/abdulrahman/splitted_files/ANN/4.txt,/home/abdulrahman/splitted_files/ANN/5.txt");
        //mr.AddSampleToDS("MyBEB", "/home/abdulrahman/splitted_files/ANN/6.txt");
        //mr.checkfordataset("MyBEB", "/home/abdulrahman/splitted_files/ANN/6.txt", true);
        //mr.DelSampleToDS("MyBEB", "/home/abdulrahman/splitted_files/ANN/6.txt");
        //mr.ListAllSamples();
        //mr.ListDSSamples("MyBEB");
    }
}
