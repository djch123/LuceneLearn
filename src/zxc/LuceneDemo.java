package zxc;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;

public class LuceneDemo {
	/*
	public static void main(String[] args) throws Exception {
		LuceneDemo demo = new LuceneDemo();
		demo.createIndex();
    }
	
	private Directory directory;
    private String[] ids = {"1", "2"};
    private String[] unIndex = {"Netherlands", "Italy"};
    private String[] unStored = {"Amsterdam has lots of bridges", "Venice has lots of canals"};
    private String[] text = {"Amsterdam", "Venice"};
    private IndexWriter indexWriter;

    private IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new StandardAnalyzer());
    //store index in memory
    public void createIndex() throws IOException {
        directory = new RAMDirectory();
        //指定将索引创建信息打印到控制台
        indexWriterConfig.setInfoStream(System.out);
        indexWriter = new IndexWriter(directory, indexWriterConfig);
        indexWriterConfig = (IndexWriterConfig) indexWriter.getConfig();
        FieldType fieldType = new FieldType();
        fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        fieldType.setStored(true);//存储
        fieldType.setTokenized(true);//分词
        for (int i = 0; i < ids.length; i++) {
            Document document = new Document();
            document.add(new Field("id", ids[i], fieldType));
            document.add(new Field("country", unIndex[i], fieldType));
            document.add(new Field("contents", unStored[i], fieldType));
            document.add(new Field("city", text[i], fieldType));
            indexWriter.addDocument(document);
        }
        indexWriter.commit();
    }
    
    //store index to filesys
    public IndexWriter getIndexWriter(String indexPath, boolean create) throws IOException {
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new StandardAnalyzer());
        if (create) {
            indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        } else {
            indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        }
        Directory directory = FSDirectory.open(Paths.get(indexPath));
        IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig);
        return indexWriter;
    }
    */
}
