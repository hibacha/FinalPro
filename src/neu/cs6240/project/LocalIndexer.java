package neu.cs6240.project;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.PorterStemFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.Version;
import org.apache.lucene.wikipedia.analysis.WikipediaTokenizer;

public class LocalIndexer {
	public static final String FILE = "/Users/zhouyf/Stack/cs6240/FinalProject/rawdata/sample.txt";
	public static final String STOPWORDS_FILE_PATH = "/Users/zhouyf/Stack/cs6240/FinalProject/rawdata/stopword.txt";
	private boolean isPageBegin = false;
	private boolean isTextBegin = false;
    private static final String OUTPUT_FILE="/Users/zhouyf/Stack/cs6240/FinalProject/rawdata/result.txt";
	
	private long docId = 0;
	private String title = "";
	private HashSet<String> stopwordset = new HashSet<String>();
	private StringBuffer pageBuffer = new StringBuffer();
    private BufferedWriter writer=null;
	
	public LocalIndexer() throws IOException{
		init();
	}
	private boolean terminate(String line) {
		return line.indexOf(Constant.TEXT_TERMINATOR)>-1;
	}

	private void init() throws IOException {
        generateStopWordsSet();
        File outputFile=new File(OUTPUT_FILE);
        if(!outputFile.exists()){
        	outputFile.createNewFile();
        }
        writer=new BufferedWriter(new FileWriter(new File(OUTPUT_FILE),true));
	}

	private void generateStopWordsSet() throws IOException {
       stopwordset.addAll(Constant.stopWords);
       File file=new File(STOPWORDS_FILE_PATH);
       BufferedReader bf=new BufferedReader(new FileReader(file));
       String line=null;
       while((line=bf.readLine())!=null){
    	   String[] words = line.split(",");
			for (String word : words) {
				stopwordset.add(word);
			}
       }
       bf.close();
	}

	private void scanWikiXML() throws IOException,
			InterruptedException {
		TokenStream tokenStream = new WikipediaTokenizer(new StringReader(
				pageBuffer.toString()));

		tokenStream = new StopFilter(Version.LUCENE_36, tokenStream,
				stopwordset);
		tokenStream = new PorterStemFilter(tokenStream);
		CharTermAttribute termAtt = tokenStream
				.addAttribute(CharTermAttribute.class);
		while (tokenStream.incrementToken()) {
			String term = termAtt.toString();

			if (isOnlyContainLetter(term)) {
				// TODO analyze term's type using typeAtt
				String strDocId=new Long(docId).toString();
				writer.write(term+"\t"+strDocId+"\n");
			}
		}
		
		tokenStream.close();
	}

	private void checkLine(String line) throws IOException, InterruptedException {
		Pattern pattern = null;
        line=line.trim();
		if (!isPageBegin) {
			if (line.startsWith("<page>"))
				isPageBegin = true;
		} else if (title.length() == 0) {
			pattern = Pattern.compile(Constant.REGEX_TITLE);
			Matcher matcher = pattern.matcher(line);
			if (matcher.find()) {
				title = matcher.group(2);
			}

		} else if (docId == 0) {
			pattern = Pattern.compile(Constant.REGEX_ID);
			Matcher matcher = pattern.matcher(line);
			if (matcher.find()) {
				docId = Integer.parseInt(matcher.group(2));
			}
		} else if (!isTextBegin) {
			if (line.startsWith(Constant.TEXT_MARK))
				isTextBegin = true;

		} else if (terminate(line)) {
			scanWikiXML();
			// reset all flag to initial status
			isPageBegin = false;
			title = "";
			docId = 0;
			isTextBegin = false;
			pageBuffer = new StringBuffer();
		} else {

			pageBuffer.append(line + " ");
		}
	}

	

	private boolean isOnlyContainLetter(String token) {
		boolean flag = true;
		for (int i = 0; i < token.length(); i++) {
			Character c = token.charAt(i);
			if (!(c <= 'z' && c >= 'a')) {
				flag = false;
				break;
			}
		}
		return flag;
	}
    public void run() throws IOException, InterruptedException{
    	File file = new File(FILE);

		BufferedReader reader = new BufferedReader(new FileReader(file));
		String line = null;
		while ((line = reader.readLine()) != null) {
             checkLine(line);
			//writer.write(line+"\n");
			System.out.println(line);
		}
		reader.close();
		writer.close();
    	
    }
	public static void main(String[] args) throws IOException, InterruptedException {
		
       LocalIndexer indexer=new LocalIndexer();
       indexer.run();
	}

}
