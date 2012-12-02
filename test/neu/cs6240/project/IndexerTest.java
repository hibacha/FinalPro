package neu.cs6240.project;

import static org.junit.Assert.assertEquals;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class IndexerTest {
	public static final String FILE = "./TestFolder/input";
	public static final String OUTPUT="./TestFolder/output";
	public static final String STOPWORDS_FILE_PATH = "/Users/zhouyf/Stack/cs6240/FinalProject/rawdata/stopword.txt";
	
	LocalIndexer indexer=null;
	@Before
	public void setUp() throws Exception {
	   indexer=new LocalIndexer(FILE,OUTPUT,STOPWORDS_FILE_PATH);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
		
		
		Pattern pattern=Pattern.compile("<(title)>([\\w]+)</\\1>");
		Matcher match=pattern.matcher("<title>anti</title>");
		
		match.find();
		String title=match.group(2);
		assertEquals(title,"anti");
		
	}
	
	@Test
	public void testInputDir() throws SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException, NoSuchFieldException, IOException{
      
	   //invoke initInputFileList method
	   Method privateMethod=LocalIndexer.class.getDeclaredMethod("initInputFileList");
       privateMethod.setAccessible(true);
       privateMethod.invoke(indexer);
       
       //get private varible inputFileList
       Field privateField=LocalIndexer.class.getDeclaredField("inputFileList");
       privateField.setAccessible(true);
      
       List<File> list=(List<File>)privateField.get(indexer);
       for(File file:list){
    	   System.out.println(file.getCanonicalPath());
       }
       
       
       Method privateGenerateWriter=LocalIndexer.class.getDeclaredMethod("generateWriter");
       privateGenerateWriter.setAccessible(true);
       privateGenerateWriter.invoke(indexer);
       
       Field privateWriteField=LocalIndexer.class.getDeclaredField("writer");
       privateWriteField.setAccessible(true);
       BufferedWriter writer=(BufferedWriter)privateWriteField.get(indexer);
       writer.write("test string");
       writer.close();

		
	}
	
	
}
