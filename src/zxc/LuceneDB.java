package zxc;

import java.io.File;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.flexible.core.nodes.RangeQueryNode;
import org.apache.lucene.queryparser.xml.builders.RangeFilterBuilder;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import com.mysql.jdbc.Connection;

public class LuceneDB {
	public static final String DBDRIVER = "com.mysql.jdbc.Driver";
	public static final String DBURL = "jdbc:mysql://localhost:3306/dblp_655";
	public static String DBUSER = "root";
	public static String DBPASS = "20120607";
	
	Connection conn = null;
	RAMDirectory memdir;
	
	public LuceneDB() {
		// TODO Auto-generated constructor stub
		try {
			Class.forName(DBDRIVER);
			conn = (Connection) DriverManager.getConnection(DBURL, DBUSER, DBPASS);
			memdir = new RAMDirectory();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		LuceneDB db = new LuceneDB();
		db.createIndex();
		db.basicSearch("network san", 5, 10);
	}
	
	public void createIndex() {
		try {
			IndexWriter writer = null;
			String sql = "SELECT * FROM publication";
			//Directory dirWrite = FSDirectory.open(new File("DBLPIndexes"));
			RAMDirectory dirWrite = memdir;
			Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_10_4);
			IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_4_10_4,analyzer);
			iwc.setOpenMode(OpenMode.CREATE);
			writer = new IndexWriter(dirWrite, iwc);
			PreparedStatement ps = conn.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				Document doc = new Document();
			    // create indexes in fields below
				doc.add(new Field("authors", rs.getString("authors"), Field.Store.YES,Field.Index.ANALYZED));
				doc.add(new Field("title", rs.getString("title"), Field.Store.YES,Field.Index.ANALYZED));
			    writer.addDocument(doc);
			}
			rs.close(); 
			conn.close(); 
			// writer.optimize(); 
			writer.close();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	public void basicSearch(String querystr, int numResultsToSkip, int numResultsToReturn) {
		try {
			//or query
			Directory dir = memdir;
			IndexReader reader = DirectoryReader.open(dir);
			IndexSearcher searcher = new IndexSearcher(reader);
			String[] queryarr = querystr.split("\\s+");
			 
			String[] queries = new String[queryarr.length * 2];
			String[] fields = new String[queries.length];
			for (int i = 0; i < queryarr.length; i++) {
				queries[i] = queryarr[i];
				queries[i + queryarr.length] = queryarr[i];
				fields[i] = "authors";
				fields[i + queryarr.length] = "title";
			}
			BooleanClause.Occur[] clauses = { BooleanClause.Occur.SHOULD, BooleanClause.Occur.SHOULD, BooleanClause.Occur.SHOULD, BooleanClause.Occur.SHOULD};  
			Query query = MultiFieldQueryParser.parse(queries, fields, clauses, new StandardAnalyzer());		 
			TopDocs topDocs = searcher.search(query, numResultsToReturn + numResultsToSkip);
			ScoreDoc[] hits = topDocs.scoreDocs;
			for (int i = 0; i < numResultsToReturn; i++) {
				int DocId = hits[i + numResultsToSkip].doc;
				Document doc = searcher.doc(DocId);
				System.out.println(doc.get("authors") +" ; "+doc.get("title")); 
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
    public List<SearchResult> spatialSearch(String query, SearchRegion region, int numResultsToSkip, int numResultsToReturn) {
    	List<SearchResult> list = new ArrayList<>();
    	try {
    		String sql = "select * from `publication` where X(location) > " + region.lx + " and X(location) < " + region.rx
        			+ " and Y(location) > " + region.ly + " and Y(location) < " + region.ry;
        	PreparedStatement pstmt = conn.prepareStatement(sql);
        	ResultSet rs = pstmt.executeQuery();
        	while (rs.next()) {
        		SearchResult sr = new SearchResult(rs.getString("title"), rs.getString("authors"), rs.getInt("year"));
        		list.add(sr);
        	}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
    	return list;
    }
	

}
