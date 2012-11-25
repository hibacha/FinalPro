package neu.cs6240.project;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Indexer {

	/**
	 * @param args
	 */
	public static final String PAGE_TAG = "page";
	public static final String FILE = "/Users/zhouyf/Stack/cs6240/FinalProject/rawdata/sample.txt";

	public static class IndexMapper extends
			Mapper<LongWritable, Text, LongWritable, Text> {

		private boolean isPageBegin = false;
		private boolean isTextBegin = false;
		
		private int docId;
		private String title = "";
        private HashSet<String> stopwordset=new HashSet<String>();
        
		private boolean checkPage(boolean isStart) {
			return false;
		}

		private void parseDocId() {

		}

		private void searchText() {

		}

		private void preprocess(String line) {

		}

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
			for (Path p : paths) {
				if (p.toString().indexOf("stopword") >= 0) {
					String header = getCacheFileContent(p.toString()).get(0);
				   String[] words=header.split(",");
				   stopwordset.addAll(Constant.stopWords);
				   for(String word:words){
					   stopwordset.add(word);
				   }
				} 
				context.write(new LongWritable(1), new Text(p.toString()));
			}
			

		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
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
				isPageBegin = false;
				title = "";
				docId = 0;
				isTextBegin = false;

			} else {
				StringTokenizer tokens = new StringTokenizer(line);
				String token;
				while (tokens.hasMoreTokens()) {
					token = tokens.nextToken();
					if(stopwordset.contains(token)){
						continue;
					}
					else if (isOnlyContainLetter(token))
						context.write(key, new Text(token));

				}
				context.write(key, new Text(line));
			}

		}
		
		private boolean isStopWord(String token){
			return false;
		}
        /**
         * eliminate non-English word
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

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        DistributedCache.addCacheFile(new Path(otherArgs[2]).toUri(), job.getConfiguration());
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
