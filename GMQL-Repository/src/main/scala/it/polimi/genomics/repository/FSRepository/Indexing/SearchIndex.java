package it.polimi.genomics.repository.FSRepository.Indexing;

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

import java.io.File;
import java.io.IOException;
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
    private StringBuilder errorMessage = new StringBuilder();
    //private boolean Success = true;

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
        this.MODE = execMode;
    }


	public String SearchLuceneIndex(String query, Directory index) throws IOException, ParseException {
		return SearchLuceneIndex(query, null, index);
	}

    public String SearchLuceneIndex(String query, String username, Directory index) throws IOException, SelectFormatException, ParseException {
        IndexReader reader;

        queryString = parseInputQuery(query);

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

        filespath = new StringBuilder();

        if (hits.length == 0) {

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
            }
        }

        Map<String, String> res = new HashMap<>();
        res.put(parameterName, filespath.toString());
        return res;
    }

    private static String parseInputQuery(String queryString) {
        logger.info("QueryString: " + queryString);
        return queryString;

    }

}

