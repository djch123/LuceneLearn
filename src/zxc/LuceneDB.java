package zxc;

import java.io.File;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
	
	public static void main(String[] args) {
		LuceneDB db = new LuceneDB();
		db.createIndex("SELECT * FROM publication");
		
		List<SearchResult> list1 = db.basicSearch("network san", 5, 10);	
		for (SearchResult sr : list1) {
			System.out.println(sr);
		}
		
		List<SearchResult> list2 = db.spatialSearch("network san", new SearchRegion(2, 1, 20, 12), 10, 5);
		for (SearchResult sr : list2) {
			System.out.println(sr);
		}
	}
	
	
	public LuceneDB() {
		// TODO Auto-generated constructor stub
		try {
			Class.forName(DBDRIVER);
			conn = (Connection) DriverManager.getConnection(DBURL, DBUSER, DBPASS);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	public void recycle() {
		try {
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public List<SearchResult> basicSearch(String querystr, int numResultsToSkip, int numResultsToReturn) {
		Directory memdir = createIndex("SELECT * FROM publication");
		return basicQuery(memdir, querystr, numResultsToSkip, numResultsToReturn);
	}
	

    public List<SearchResult> spatialSearch(String querystr, SearchRegion region, int numResultsToSkip, int numResultsToReturn) {
    	try {
    		String sql = "select * from `publication` where X(location) >= " + region.lx + " and X(location) <= " + region.rx
        			+ " and Y(location) >= " + region.ly + " and Y(location) <= " + region.ry;
        	Directory memdir = createIndex(sql);
        	return basicQuery(memdir, querystr, numResultsToSkip, numResultsToReturn);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
    	return null;
    }
    
    public Directory createIndex(String sql) {
		RAMDirectory dirWrite = new RAMDirectory();
		try {
			IndexWriter writer = null;
			//Directory dirWrite = FSDirectory.open(new File("DBLPIndexes"));
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
				doc.add(new Field("year", rs.getString("year"), Field.Store.YES,Field.Index.ANALYZED));
			    writer.addDocument(doc);
			}
			rs.close(); 
			// writer.optimize(); 
			writer.close();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return dirWrite;
	}
	
	public List<SearchResult> basicQuery(Directory dir, String querystr, int numResultsToSkip, int numResultsToReturn) {
		List<SearchResult> list = new ArrayList<>();
		try {
			//Directory dir = memdir;
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
			for (int i = 0; i < Math.min(numResultsToReturn, hits.length); i++) {
				int DocId = hits[i + numResultsToSkip].doc;
				Document doc = searcher.doc(DocId);
				//System.out.println(doc.get("authors") +" ; "+doc.get("title")); 
				String authors = doc.get("authors");
				String title = doc.get("title");
				int year = Integer.valueOf(doc.get("year"));
				list.add(new SearchResult(title, authors, year));
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return list;
	}

}
