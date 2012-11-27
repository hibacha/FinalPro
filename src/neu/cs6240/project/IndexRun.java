package neu.cs6240.project;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

import org.apache.lucene.analysis.PorterStemFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.Version;
import org.apache.lucene.wikipedia.analysis.WikipediaTokenizer;
import org.apache.tika.exception.TikaException;
import org.xml.sax.SAXException;

public class IndexRun {

	/**
	 * @param args
	 * @throws TikaException
	 * @throws SAXException
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Set<String> stopWords = new HashSet<String>();
		stopWords.addAll(Constant.stopWords);
		String test = "{{Redirect|Anarchist|the fictional character|Anarchist (comics)}} '''Anarchism''' is generally defined as a "
				+ "[[political philosophy]] which "
				+ "holds the [[state (polity)|state]] to "
				+ "be undesirable, unnecessary, "
				+ "or harmful,&lt;ref name=&quot;definition&quot;&gt;";
		
		TokenStream tokenStream = new WikipediaTokenizer(new StringReader(test));
		
		tokenStream = new StopFilter(Version.LUCENE_36, tokenStream, stopWords);
		tokenStream = new PorterStemFilter(tokenStream);
		CharTermAttribute termAtt = tokenStream
				.addAttribute(CharTermAttribute.class);
		TypeAttribute typeAtt = tokenStream.addAttribute(TypeAttribute.class);
		while (tokenStream.incrementToken()) {
			System.out.println(termAtt.toString() + ":" + typeAtt.type());
		}

		tokenStream.close();
		Hashtable<Long, Long> hashtable = new Hashtable<Long, Long>();
		Long long1=new Long(90L);
		Long long2=new Long(90L);
		if(hashtable.get(long1)==null){
			hashtable.put(long1, 1L);
		}
		
		if(hashtable.get(long1)==null){
			hashtable.put(long1, 1L);
		}else{
			
			hashtable.put(long1, hashtable.get(long1)+5);
		}
		System.out.println(hashtable.get(long2));
	}
	
	
	
	
	
}
