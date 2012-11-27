package neu.cs6240.project;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.lucene.analysis.PorterStemFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.Version;
import org.apache.lucene.wikipedia.analysis.WikipediaTokenizer;

public class Indexer {

	/**
	 * @param args
	 */
	public static final String PAGE_TAG = "page";
	public static final String FILE = "/Users/zhouyf/Stack/cs6240/FinalProject/rawdata/sample.txt";

	public static class IndexMapper extends
			Mapper<LongWritable, Text, Text, LongWritable> {

		private boolean isPageBegin = false;
		private boolean isTextBegin = false;

		private long docId=0;
		private String title = "";
		private HashSet<String> stopwordset = new HashSet<String>();
		private StringBuffer sb = new StringBuffer();

		private int counter = 0;

		private boolean terminate(String line) {
			return line.endsWith(Constant.TEXT_TERMINATOR);
		}

		private ArrayList<String> getCacheFileContent(String localPath)
				throws IOException {

			ArrayList<String> rst = new ArrayList<String>();
			BufferedReader fis = null;
			try {
				fis = new BufferedReader(new FileReader(localPath));
				String line = "";
				while ((line = fis.readLine()) != null) {
					rst.add(line);
				}
				fis.close();
			} catch (FileNotFoundException io) {

			}
			return rst;
		}

		@Override
		/**
		 * read from cache to get header
		 * read from cache to get species names
		 */
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Path[] paths = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());
			stopwordset.addAll(Constant.stopWords);
			for (Path p : paths) {
				if (p.toString().indexOf("stopword") >= 0) {
					String header = getCacheFileContent(p.toString()).get(0);
					String[] words = header.split(",");
					for (String word : words) {
						stopwordset.add(word);
					}
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
		}

		private void wikiTokenizer(Context context) throws IOException,
				InterruptedException {
			TokenStream tokenStream = new WikipediaTokenizer(new StringReader(
					sb.toString()));

			tokenStream = new StopFilter(Version.LUCENE_36, tokenStream,
					stopwordset);
			tokenStream = new PorterStemFilter(tokenStream);
			CharTermAttribute termAtt = tokenStream
					.addAttribute(CharTermAttribute.class);
			TypeAttribute typeAtt = tokenStream
					.addAttribute(TypeAttribute.class);
			while (tokenStream.incrementToken()) {
				String term = termAtt.toString();

				if (isOnlyContainLetter(term)) {
					// TODO analyze term's type using typeAtt
					context.write(new Text(termAtt.toString()),
							new LongWritable(docId));
				}
			}
			context.write(new Text(title), new LongWritable(docId));
			tokenStream.close();
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			Pattern pattern = null;
			String line = value.toString().toLowerCase().trim();

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
				wikiTokenizer(context);
				//reset all flag to initial status
				isPageBegin = false;
				title = "";
				docId = 0;
				isTextBegin = false;
				sb = new StringBuffer();
			} else {

				sb.append(line + " ");

				// StringTokenizer tokens = new StringTokenizer(line);
				// String token;
				// while (tokens.hasMoreTokens()) {
				// token = tokens.nextToken();
				// if(stopwordset.contains(token)){
				// continue;
				// }
				// //include wiki bold token
				// else
				// if(token.startsWith(Constant.WIKI_BOLD)&&token.endsWith(Constant.WIKI_BOLD)){
				// context.write(key,new
				// Text(token.substring(3,token.length()-3)));
				// }
				// else if (isOnlyContainLetter(token)){
				// context.write(key, new Text(token));}
				// }
				// context.write(key, new Text(line));
			}

		}

		/**
		 * eliminate non-English word
		 * 
		 * @param token
		 * @return
		 */
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
	}

	public static class Reduce extends Reducer<Text, LongWritable, Text, Text> {

		private Hashtable<Long, Long> hashtable = new Hashtable<Long, Long>();

		@Override
		protected void reduce(Text term, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			Hashtable<Long, Long> hashtable = new Hashtable<Long, Long>();
			Iterator<LongWritable> doIds = values.iterator();
			// TODO memory efficiency
			
			while (doIds.hasNext()) {
				LongWritable id = doIds.next();
				if (hashtable.get(new Long(id.get()))==null) {
					hashtable.put(id.get(), 1L);
				} else {
					long count = hashtable.get(id.get()).longValue();
					count++;
					hashtable.put(id.get(), new Long(count));
				}
			}
			Enumeration<Long> enumberator = hashtable.keys();
			while (enumberator.hasMoreElements()) {
				Long docId = enumberator.nextElement();
				context.write(term,
						new Text(docId.toString() + ":" + hashtable.get(docId)));
			}
			
			hashtable = new Hashtable<Long, Long>();
		}

	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		System.out.println("team");
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Index");
		job.setJarByClass(Indexer.class);
		// parse generic argument like third party library
		GenericOptionsParser optionParser = new GenericOptionsParser(
				job.getConfiguration(), args);
		// user-specific argument
		String[] otherArgs = optionParser.getRemainingArgs();
		job.setMapperClass(IndexMapper.class);
        job.setReducerClass(Reduce.class);
        
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		// must get configuration from job
		DistributedCache.addCacheFile(new Path(otherArgs[2]).toUri(),
				job.getConfiguration());
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
