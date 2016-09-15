package it.polimi.genomics.repository.Index;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import it.polimi.genomics.repository.util.Utilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Created by Abdulrahman Kaitoua on 04/06/15.
 * Email: abdulrahman.kaitoua@polimi.it
 */
public class LuceneIndex {

    private static final Logger logger = LoggerFactory.getLogger(LuceneIndex.class);

    /**
     *
     * @param indexP
     * @param SampleUrl
     * @param id
     * @param HDFSfile
     */
    public static void addSampletoIndex(String indexP, String SampleUrl, int id, boolean HDFSfile) {
        buildIndex(indexP, SampleUrl, id, false, HDFSfile);
    }

    public static void addSampletoInMemIndex(String indexP, String SampleUrl, int id, boolean HDFSfile) {
        buildIndex(indexP, SampleUrl, id, false, HDFSfile);
    }

    /**
     *
     * @param indexP
     * @param id
     */
    public static void deleteIndexedSamplebyID(String indexP, int id) {
        IndexWriter indexWriter;
        try {
            logger.info(
                    "Index directory:" + indexP + "\n\t Deleting index of ID : " + id);

            Directory dir = FSDirectory.open(new File(indexP));
            // :Post-Release-Update-Version.LUCENE_XY:
            Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);
            IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_47, analyzer);
            indexWriter = new IndexWriter(dir, iwc);

            indexWriter.deleteDocuments(new Term("id", id + ""));
            indexWriter.close();
        } catch (Exception ex) {

        }
    }

    /**
     *
     * @param indexP
     * @param URL
     */
    public static void deleteIndexedSamplebyURL(String indexP, String URL) {
        IndexWriter indexWriter;
        try {
            logger.info(
                    "Index directory:" + indexP + "\n\t Deleting index of url : " + URL);

            Directory dir = FSDirectory.open(new File(indexP));
            // :Post-Release-Update-Version.LUCENE_XY:
            Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);
            IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_47, analyzer);
            indexWriter = new IndexWriter(dir, iwc);

            indexWriter.deleteDocuments(new Term("url", URL));
            indexWriter.close();
        } catch (Exception ex) {

        }
        String ss = "";

    }

    /**
     *
     * @param indexP
     */
    public static void printIndex(String indexP) {
        try {
            logger.info(
                    "Index directory:" + indexP + "\n\t Print All the data in the index ..... ");

            Directory dir = FSDirectory.open(new File(indexP));
            // :Post-Release-Update-Version.LUCENE_XY:
            Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);

            //QueryParser parser = new QueryParser(Version.LUCENE_47, "id", analyzer);
            //Query query = parser.parse("id:*");
            IndexReader reader = DirectoryReader.open(dir);
            for (int i = 0; i < reader.numDocs(); i++) {
                Document d = reader.document(i);
                logger.info(
                        "The ID :" + d.getField("id") + "\n\t The meta:\n\t"
                                + d.getField("meta") + "\n\tThe Path:\n\t" + d.getField("url"));
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    /**
     *
     * @param dir
     */
    public static void printIndex(Directory dir) {
        try {
            Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);

            //QueryParser parser = new QueryParser(Version.LUCENE_47, "id", analyzer);
            //Query query = parser.parse("id:*");
            IndexReader reader = DirectoryReader.open(dir);
            for (int i = 0; i < reader.numDocs(); i++) {
                Document d = reader.document(i);
                logger.info(
                        "The ID :" + d.getField("id") + "\n\t The meta:\n\t"
                                + d.getField("meta") + "\n\tThe Path:\n\t" + d.getField("url"));
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    /**
     *
     * @param indexP
     * @param id
     */
    public static void searchIndexedbyID(String indexP, int id) {


        try{
            Directory dir = FSDirectory.open(new File(indexP));
            IndexReader reader = DirectoryReader.open(dir);
            searchIndexedbyID(reader,id);
        }catch(IOException ioe){

        }

    }

    public static void searchIndexedbyIDInMem(Directory dir, int id) {
        try {

            IndexReader reader = IndexReader.open(dir);
            searchIndexedbyID(reader,id);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    public static void searchIndexedbyID(IndexReader reader, int id) {
        try {
            logger.info("Search index by ID : " + id);

            Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);

            QueryParser parser = new QueryParser(Version.LUCENE_47, "id", analyzer);
            Query query = parser.parse(id + "");
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs collector = searcher.search(query, 500);
            ScoreDoc[] hits = collector.scoreDocs;
            if (hits.length > 0) {
                Document d = searcher.doc(hits[0].doc);
                logger.info(
                        "The ID :" + d.getField("id") + "\n\t The meta:\n\t"
                                + d.getField("meta") + "\n\tThe Path:\n\t" + d.getField("url"));
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     *
     * @param indexP
     * @param SampleUrl
     * @param id
     * @param create
     * @param HDFSfile
     */
    public static void buildIndex(String indexP, String SampleUrl, int id, boolean create, boolean HDFSfile) {
        final File inputfile = new File(SampleUrl);

        IndexWriter indexWriter;
        BufferedReader reader;
        String row = null;
        StringBuilder meta = new StringBuilder();

        if (HDFSfile) {

        } else if (!inputfile.exists() || !inputfile.canRead()) {
            logger.warn(
                    "Document directory '" + inputfile.getAbsolutePath()
                            + "' does not exist or is not readable, please check the path");
            System.exit(1);
        }
        logger.info(
                "Indexing: " + SampleUrl + " \n\tTo directory:" + indexP);

        try {
            Directory dir = FSDirectory.open(new File(indexP));
            // :Post-Release-Update-Version.LUCENE_XY:
            Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);
            IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_47, analyzer);

            if (create) {
                // Create a new index in the directory, removing any
                // previously indexed documents:
                iwc.setOpenMode(OpenMode.CREATE);
            } else {
                // Add new documents to an existing index:
                iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
            }
            indexWriter = new IndexWriter(dir, iwc);

            // Creating BufferReader object and specifying the path of the file
            //whose data is required to be indexed.
            if (HDFSfile) {
                Configuration conf = Utilities.getInstance().gethdfsConfiguration();
                FileSystem fs = FileSystem.get(conf);
                reader = new BufferedReader(
                        new InputStreamReader(fs.open(new org.apache.hadoop.fs.Path(SampleUrl)), "UTF-8"));
            } else {
                reader = new BufferedReader(
                        new InputStreamReader(new FileInputStream(inputfile), "UTF-8"));
            }
            // Reading each line present in the file.
            while ((row = reader.readLine()) != null) {

                // Getting each field present in a row into an Array and file delimiter is "space separated"
                String Arow[] = row.split("\t");
                //System.out.println(row);
                meta.append(Arow[0].replace(' ', '_').replaceAll("\\W+", "_"));
                meta.append("_");
                Arow[1] = Arow[1].replace(' ', '_').replaceAll("\\W+", "_");
                meta.append(Arow[1]);
                meta.append("\t");
            }

            Document document = new Document();
            document.add(new StringField("id", id + "", Field.Store.YES));

            document.add(new TextField("meta", meta.toString(), Field.Store.YES));
            document.add(new StringField("url", inputfile.toString().substring(0, inputfile.toString().length() - 5), Field.Store.YES));
            indexWriter.addDocument(document);
            logger.info(
                    "Finished indexing...");
            indexWriter.close();
            reader.close();
            File f = new File(indexP);
            f.setExecutable(true, false);
            f.setReadable(true, false);
            f.setWritable(true, false);
            File[] files = f.listFiles();
            for (File f1 : files) {
                f1.setExecutable(true, false);
                f1.setReadable(true, false);
                f1.setWritable(true, false);
            }
        } catch (IndexOutOfBoundsException ex) {
            logger.error(
                    "The meta Data file has to be in the style of (Att TAB_SEPARATOR value)");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     *
     * @param inputfile  which is the file of the sample
     * @param id    the id to be set for this sample
     * @param HDFSfile  boolean indicates if this will be a sample file on Hadoop or local file system
     * @author Abdulraman Kaitoua (A. Kaitoua)
     * @return
     */
    public static Directory buildInMemIndex(File inputfile, int id, Directory inDir, boolean HDFSfile) {

        IndexWriter indexWriter;
        BufferedReader reader;
        String row = null;
        StringBuilder meta = new StringBuilder();

        if (HDFSfile) {

        } else if (!inputfile.exists() || !inputfile.canRead()) {
            logger.warn("Meta Data directory '" + inputfile.getAbsolutePath()
                    + "'\n\t\tDoes not exist or is not readable, please check the path.\n\t\tThis file will not be included for processing");
            return inDir;
        }

        Directory dir = inDir;

        try {
            // :Post-Release-Update-Version.LUCENE_XY:
            Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);
            IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_47, analyzer);

            if (dir == null) {
                dir = new RAMDirectory();
                iwc.setOpenMode(OpenMode.CREATE);
            } else {
                iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
            }
            indexWriter = new IndexWriter(dir, iwc);

            if (HDFSfile) {
                Configuration conf = new Configuration();
//                conf.addResource(new org.apache.hadoop.fs.Path(Utilities.getInstance().CoreConfigurationFiles));
//                conf.addResource(new org.apache.hadoop.fs.Path(Utilities.getInstance().HDFSConfigurationFiles));
                FileSystem fs = FileSystem.get(conf);
                reader = new BufferedReader(
                        new InputStreamReader(fs.open(new org.apache.hadoop.fs.Path(inputfile.getPath())), "UTF-8"));
            } else {
                reader = new BufferedReader(
                        new InputStreamReader(new FileInputStream(inputfile), "UTF-8"));
            }
            // Reading each line present in the file.
            while ((row = reader.readLine()) != null) {

                // Getting each field present in a row into an Array and file delimiter is "space separated"
                String Arow[] = row.split("\t");
                //System.out.println(row);
                meta.append(Arow[0].replace(' ', '_').replaceAll("\\W+", "_"));
                meta.append("_");
                Arow[1] = Arow[1].replace(' ', '_').replaceAll("\\W+", "_");
                meta.append(Arow[1]);
                meta.append("\t");
            }

            Document document = new Document();
            document.add(new StringField("id", id + "", Field.Store.YES));
            document.add(new TextField("meta", meta.toString(), Field.Store.YES));
            document.add(new StringField("url", inputfile.toString().substring(0, inputfile.toString().length() - 5), Field.Store.YES));
            indexWriter.addDocument(document);
            logger.info("Finished indexing..." + inputfile.toString());
            indexWriter.close();
            reader.close();

        } catch (IndexOutOfBoundsException ex) {
            logger.error("The meta Data file has to be in the style of (Att TAB_SEPARATOR value)");
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            return dir;
        }
    }
}

