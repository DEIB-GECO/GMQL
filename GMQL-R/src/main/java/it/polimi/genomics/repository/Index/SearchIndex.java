package it.polimi.genomics.repository.Index;

import it.polimi.genomics.core.exception.SelectFormatException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

//import orchestrator.util.Utilities;

/**
 * Created by Abdulrahman Kaitoua on 04/06/15.
 * Email: abdulrahman.kaitoua@polimi.it
 */
public class SearchIndex {

    //    public static String  RepoDir = "/home/gql_repository/data/";
//    public static String  HDFSRepoDir = "/user/";
    private static final Logger logger = LoggerFactory.getLogger(SearchIndex.class);
    private final Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);
    //private String indexPath = "/tmp/index/";
    private String inputDirectory = "/user/abdulrahman/splitted_encode_data/";
    //private String queries = null;
    private String queryString = null;
    //private int hitsnum = 1000;
    private static final String Localexe = "-x local", MapRedExec = "-x mapreduce";
    private String MODE;
    //private String Pigexec = MapRedExec;
    //private String pig_filepath = "/home/abdulrahman/opt_query.pig";
    //private final LinkedList<String[]> fileQueries = new LinkedList<>();
    //private static String param = "";
    private StringBuilder errorMessage = new StringBuilder();
    //private boolean Success = true;
	/*
    String usage = "lucene.Search "
            + " [-index INDEX_PATH] [-dataPath] [-queries true] "
            + "[-query QUERY_STRING] [-hits numberOfHits] "
            + "[-exec local|mapreduce]"
            + " [-pig pig_filepath]\n\n"
            + "This search the INDEX located in the INDEX_PATH using QUERY_STRING or queries loaded from Queriesfile path\n\n";
	*/

	/*
    public SearchIndex() {

    }
    */

	public SearchIndex(String HDFSDir, String username, String execMode) {
		this(null, HDFSDir + username + "/regions/", null, null, execMode, null);
	}


	public SearchIndex(String repoDir, String HDFSDir, String username, String execMode, String pigPath) {
		this(repoDir + username + "/indexes/", HDFSDir + username + "/regions/", "true", null, execMode, pigPath);
	}

    public SearchIndex(String indexDir, String HDFSdir, String queries, String query, String execMode, String pigpath) {
        //this.indexPath = indexDir;
        this.inputDirectory = HDFSdir;
        //this.queries = queries;
        this.queryString = query;
        //this.pig_filepath = pigpath;
        this.MODE = execMode;
        //this.Pigexec = execMode.toLowerCase().equals("local") ? Localexe : MapRedExec;
    }


	public String SearchLuceneIndex(String query, Directory index) throws IOException, ParseException {
		return SearchLuceneIndex(query, null, index);
	}

    public String SearchLuceneIndex(String query, String username, Directory index) throws IOException, SelectFormatException, ParseException {
        IndexReader reader;

        queryString = parseInputQuery(query);
        //indexPath = Utilities.getInstance().RepoDir + username + "/indexes/" + fileQueries.get(j)[1].split("/")[1];

        if (this.MODE.toLowerCase().equals("local")) {
            inputDirectory = "";
        }

        reader = IndexReader.open(index);

        Map<String, String> res = SearchLuceneIndex(reader, "param", 1);

        reader.close();

        if(res != null)
        return res.get("param");
        else return null;
    }

    public Map<String, String> SearchLuceneIndex(IndexReader reader, String parameterName, int j) throws IOException, SelectFormatException, ParseException {

        int hitsPerPage = 50000;
        IndexSearcher searcher;
        QueryParser parser;
        Query query;
        TopDocs collector;
        StringBuilder filespath;

        searcher = new IndexSearcher(reader);
        parser = new QueryParser(Version.LUCENE_47, "meta", analyzer);
        query = parser.parse(queryString);

        logger.info("Searching for: " + query.toString("meta") + " " + query.toString());
        collector = searcher.search(query, hitsPerPage);
        ScoreDoc[] hits = collector.scoreDocs;

        logger.info("Query:" + j + "\tFound " + hits.length + " hits.");
        //int docIdd = hits[0].doc;
        //Document dd = searcher.doc(docIdd);
        //System.out.println("dd.getField(\"url\").stringValue() = " + dd.getField("url").stringValue());
        
        filespath = new StringBuilder();

        if (hits.length == 0) {
//            String message = "Query number ( " + j + " ) has no hits in your search.\n\t"
//                    + "Please change the input query."
//                    + "\n\tCheck select : " + parameterName.split("_")[1];
            //logger.warn(message);
            //errorMessage.append(message);
            return null;
        }

        Document d;
        d = searcher.doc(hits[0].doc);
        if (MODE.equals("LOCAL")) {
            File sample = new File(d.getField("url").stringValue());
            if (!sample.exists()) {
                logger.warn("This sample is not found on local mode: " + d.getField("url").stringValue()
                        + "\n\t Please check the file location and availability.");
            }
        }
        filespath.append(inputDirectory);
		filespath.append(d.getField("url").stringValue());

        if (hits.length > 1) {
            for (int i = 1; i < hits.length; ++i) {
                int docId = hits[i].doc;
                d = searcher.doc(docId);
                if (MODE.equals("LOCAL")) {
                    File sample = new File(d.getField("url").stringValue());
                    if (!sample.exists()) {
                        logger.warn("This sample is not found on local mode: " + d.getField("url").stringValue()
                                + "\n\t Please check the file location and availability.");
                    }
                }
                filespath.append(",");
                filespath.append(inputDirectory);
				filespath.append(d.getField("url").stringValue());
                // System.out.println((i + 1) + ". " + d.getField("id").stringValue()+"\t"+ d.getField("att").stringValue()+" \t"+d.getField("val").stringValue());
            }
        }

        Map<String, String> res = new HashMap<>();
        res.put(parameterName, filespath.toString());
        return res;

        // close the reader when there is no need
        //System.out.println("INFO: \t" + param + "\n");
        //executepig(pig_filepath, param, Pigexec);
        // BooleanQuery query = new BooleanQuery();
//            PhraseQuery meta = new PhraseQuery();
//            //PhraseQuery val = new PhraseQuery();
//
//            //Term t= new Term("att", "dataVersion".toLowerCase());
//            Term t = new Term("meta", "lab:uw".toLowerCase());
//            meta.add(t);
        //val.add(t1);
        //query.add(meta,BooleanClause.Occur.MUST);
        //query.add(val,BooleanClause.Occur.MUST);
        // System.out.println(query.toString());
    }

	/*
    public boolean getSuccessStatus() {
        return Success;
    }

    public String getExeMessage() {
        return errorMessage.toString();
    }
    */

    private static String parseInputQuery(String queryString) {
        // change the format of the filter query to work on lucene analyzer
        //[antibody:IRF1]+([lab:UT-A]*[cell:K562])
        //type_genes NOT provider_UCSC
        //provider* AND NOT provider_UCSC

        //String line = queryString == null ? "(antibody_IRF1) OR ((lab_UT_A) AND (cell_K562))".trim() : queryString.trim();
        //queryString = clearData(line);

//        //change the * and + to and and or
//        for (int i = 0; i < queryString.length(); i++) {
//            int ind = queryString.indexOf('+');
//            while (ind > 0) {
//                String s = queryString.substring(0, ind);
//                s += " OR ";
//                s += queryString.substring(ind + 1, queryString.length());
//                queryString = s;
//                ind = queryString.indexOf('+');
//            }
//            ind = queryString.indexOf('*');
//            while (ind > 0) {
//                String s = queryString.substring(0, ind);
//                s += " AND ";
//                s += queryString.substring(ind + 1, queryString.length());
//                queryString = s;
//                ind = queryString.indexOf('*');
//            }
//        }
        //queryString = queryString.replace('?', '*');
        logger.info("QueryString: " + queryString);
        return queryString;

    }
/*
    private static String clearData(String line) {
        String queryString = line.replace(' ', '_');
        queryString = queryString.replaceAll("\\W+", "_");
        return queryString;
    }
*/

	/*
    public void executepig(String pigScript, String inputparam) {

        String s = null;

        try {

            // run the Unix "ps -ef" command
            // using the Runtime exec method:
            String command = "/usr/local/pig/bin/pig " + this.Pigexec + " -f " + pigScript + inputparam;
            System.out.println(command);

            Process p = Runtime.getRuntime().exec(command);

            BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));

            BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));

            // read the output from the command
            logger.info("\t\t............ Under progress ............\n");
            while ((s = stdInput.readLine()) != null) {
                System.out.println(s);
            }

            // read any errors from the attempted command
            logger.error("\"\tHere is the standard error of the command (if any):\n");
            while ((s = stdError.readLine()) != null) {
                System.out.println(s);
            }

            //System.exit(0);
        } catch (IOException e) {
            logger.error(
                    "This Query is not going to continue.. See the system admin.. ");
            logger.error(
                    e.getMessage(), e);

            System.exit(-1);
        }
        //return "sb.toString()";
    }
    */
}

