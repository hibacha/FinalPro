package neu.cs6240.project;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IndexerTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
		
		Pattern pattern=Pattern.compile("<(title)>([\\w]+)</\\1>");
		Matcher match=pattern.matcher("<title>anti</title>");
		
		match.find();
		System.out.println(match.group(2));
		
	}
}
