package neu.cs6240.project;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.XMPDM;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.xml.XMLParser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.SAXException;

public class IndexRun {

	/**
	 * @param args
	 * @throws TikaException 
	 * @throws SAXException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException, SAXException, TikaException {
		// TODO Auto-generated method stub
		InputStream stream = new FileInputStream(new File(""));
		BodyContentHandler handler = new BodyContentHandler(System.out);
		Parser parser = new XMLParser();
		BufferedReader bs = new BufferedReader(new InputStreamReader(stream));
		String line;
//		while ((line = bs.readLine()) != null) {
//			// System.out.println(line);
//		}
		Metadata meta = new Metadata();
	meta.set("pigu", "ji");
		ParseContext context = new ParseContext();
		parser.parse(stream, handler, meta, context);
		System.out.println(handler.toString());
		meta.set(XMPDM.ALBUM , new String[]{"ab","dd"});
		for(String name:meta.names()){
			System.out.println("name:"+name+"   "+meta.get(name));
		}
		 final String dir = System.getProperty("user.dir");
	        System.out.println("current dir = " + dir);
	        
	        File f=new File(new File("/Users/zhouyf/cs6240"),"./file.html");
	        System.out.println(f.exists());
	}

}
