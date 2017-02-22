package zxc;

public class SearchResult {	
	String title;
	String author;
	int year;
	public SearchResult(String title, String author, int year) {
		super();
		this.title = title;
		this.author = author;
		this.year = year;
	}
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "title:" + title + ",author:" + author + ",year:" + year;
	}
}
