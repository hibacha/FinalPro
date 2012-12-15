package neu.cs6240.project;
/*head*/
import java.util.Arrays;
import java.util.List;

public class Constant {

	/**
	 * @param args
	 */
	public static final long INIT_DOCID=-1;
	public static final String PAGE_MARK="page";
	public static final String REGEX_TITLE="<(title)>(.*)</\\1>";
	public static final String REGEX_ID="<(id)>(.*)</\\1>";
	public static final String TEXT_MARK="<text";
	public static final String TEXT_TERMINATOR="</text>";
	public static final String WIKI_BOLD="'''";
	public static final List<String> stopWords = Arrays.asList(
		      "a", "an", "and", "are", "as", "at", "be", "but", "by",
		      "for", "if", "in", "into", "is", "it",
		      "no", "not", "of", "on", "or", "such",
		      "that", "the", "their", "then", "there", "these",
		      "they", "this", "to", "was", "will", "with"
		    );
}



// master ok