package neu.cs6240.project;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.lucene.analysis.PorterStemFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.Version;
import org.apache.lucene.wikipedia.analysis.WikipediaTokenizer;

public class LocalIndexer {
	private boolean isPageBegin = false;
	private boolean isTextBegin = false;
    
	//store document id
	private long docId = Constant.INIT_DOCID;
	
	//store document title
	private String title = "";
	
	//store stop word into hash set
	private HashSet<String> stopwordset = new HashSet<String>();
	
	//store the wiki page content in buffer
	private StringBuffer pageBuffer = new StringBuffer();

	//writer for generating result
	private BufferedWriter writer = null;

	private String inputDir;
	private String outputDir;
	
	//file path of stop words 
	private String stopwordsFile;
	private List<File> inputFileList = new ArrayList<File>();

	public LocalIndexer(String inputDir, String outputDir, String stopwordsFile)
			throws Exception {
		this.inputDir = inputDir;
		this.outputDir = outputDir;
		this.stopwordsFile = stopwordsFile;
		init();
	}

	private boolean terminate(String line) {
		return line.indexOf(Constant.TEXT_TERMINATOR) > -1;
	}

	private void init() throws Exception {

		formalizeOutputDir();
		initInputFileList();
		generateStopWordsSet();
		checkOutputDir();
		// get last input file name
		generateWriter();
	}

	private void formalizeOutputDir() {
		// TODO check outputDir has end separator
		String seperator = System.getProperty("file.separator");
		if (!outputDir.endsWith(seperator)) {
			outputDir += seperator;
		}
	}

	private void initInputFileList() throws FileNotFoundException {
		File inputBase = new File(inputDir);
		// check if input file or directory exists
		if (!inputBase.exists()) {
			throw new FileNotFoundException("Sorry! Input Path is not valid!");
		}
		if (inputBase.isFile()) {
			inputFileList.add(inputBase);
		} else {
			for (File file : inputBase.listFiles()) {
				if (file.isFile() && !file.isHidden()) {
					inputFileList.add(file);
				}
			}
		}
	}

	private void checkOutputDir() throws Exception {
		if (!(new File(outputDir).isDirectory())) {
			throw new Exception("output is not a directory!");
		}
	}

	private void generateWriter() throws IOException {

		if (inputFileList.size() == 0)
			return;
		String newFilePath = "";
		File tail = inputFileList.get(inputFileList.size() - 1);
		String fileName = tail.getName();

		newFilePath = outputDir + fileName + ".rst";
		File outputFile = new File(newFilePath);
		if (!outputFile.exists()) {
			outputFile.createNewFile();
		}
		writer = new BufferedWriter(new FileWriter(outputFile, false));

	}

	private void generateStopWordsSet() throws IOException {
		stopwordset.addAll(Constant.stopWords);
		File file = new File(stopwordsFile);
		BufferedReader bf = null;
		try {
			bf = new BufferedReader(new FileReader(file));
			String line = null;
			while ((line = bf.readLine()) != null) {
				String[] words = line.split(",");
				for (String word : words) {
					stopwordset.add(word);
				}
			}
		} catch (FileNotFoundException e) {
			bf.close();
		}

	}

	private void scanWikiXML() throws IOException, InterruptedException {
		// generate token stream
		TokenStream tokenStream = new WikipediaTokenizer(new StringReader(
				pageBuffer.toString()));
		Hashtable<String, Long> hashCount = new Hashtable<String, Long>();
		// apply stop words filter on token stream
		tokenStream = new StopFilter(Version.LUCENE_36, tokenStream,
				stopwordset);

		// apply stemming filter to token stream
		tokenStream = new PorterStemFilter(tokenStream);
		CharTermAttribute termAtt = tokenStream
				.addAttribute(CharTermAttribute.class);

		TypeAttribute typeAtt = tokenStream.addAttribute(TypeAttribute.class);
		while (tokenStream.incrementToken()) {
			String term = termAtt.toString();

			if (isOnlyContainLetter(term)) {
				// TODO analyze term's type using typeAtt
				String type = typeAtt.type();
				// skip the Internal Link and citation
				if (!type.equals(WikipediaTokenizer.INTERNAL_LINK)
						&& !type.equals(WikipediaTokenizer.CITATION)
						&& !type.equals(WikipediaTokenizer.EXTERNAL_LINK)) {
					addCount(hashCount, term);
				}
			}
		}
		// emit all term with its belonged docId and occur times
		Enumeration<String> enumerator = hashCount.keys();
		while (enumerator.hasMoreElements()) {
			String term = enumerator.nextElement();
			writer.write(docId + "\t" + term + "\t" + hashCount.get(term)
					+ "\n");
		}
		tokenStream.close();
	}

	private void addCount(Hashtable<String, Long> table, String term) {
		if (table.containsKey(term)) {
			table.put(term, new Long(table.get(term) + 1));
		} else {
			table.put(term, new Long(1));
		}
	}

	private void checkLine(String line) throws IOException,
			InterruptedException {
		Pattern pattern = null;
		// important trim before parsing
		line = line.toLowerCase().trim();
		if (!isPageBegin) {
			if (line.startsWith("<page>"))
				isPageBegin = true;
		} else if (title.length() == 0) {
			pattern = Pattern.compile(Constant.REGEX_TITLE);
			Matcher matcher = pattern.matcher(line);
			if (matcher.find()) {
				title = matcher.group(2);
				pageBuffer.append(title + " ");
			}

		} else if (docId == Constant.INIT_DOCID) {
			pattern = Pattern.compile(Constant.REGEX_ID);
			Matcher matcher = pattern.matcher(line);
			if (matcher.find()) {
				docId = Integer.parseInt(matcher.group(2));
			}
		} else if (!isTextBegin) {

			if (line.startsWith(Constant.TEXT_MARK) && !terminate(line)) {
				isTextBegin = true;
			} else if (line.startsWith(Constant.TEXT_MARK) && terminate(line)) {
				// indicating one line text content, we can discard
				
				//scan only for title
				scanWikiXML();
				resetFlag();
			}

		} else if (terminate(line)) {

			scanWikiXML();
			// reset all flag to initial status
			resetFlag();
		} else {
			pageBuffer.append(line + " ");
		}
	}

	private void resetFlag() {
		isPageBegin = false;
		title = "";
		docId = Constant.INIT_DOCID;
		isTextBegin = false;
		pageBuffer = new StringBuffer();
		System.gc();
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

	public void run() throws IOException, InterruptedException {

		File file = null;
		BufferedReader reader = null;
		while (inputFileList.size() > 0) {
			file = inputFileList.remove(inputFileList.size() - 1);
			reader = new BufferedReader(new FileReader(file));
			String line = null;
			while ((line = reader.readLine()) != null) {
				checkLine(line);
			}
			//close previous buffer writer to make sure remaining flush to file
			writer.close();
			generateWriter();
		}
		reader.close();
		
	}

	public static void main(String[] args) throws Exception {

		CommandLineParser line = new PosixParser();
		Options options = new Options();

		@SuppressWarnings("static-access")
		Option dir = OptionBuilder.isRequired().hasArg()
				.withArgName("DIR NAME").withLongOpt("directory")
				.withDescription("specify directory containing wiki dump file")
				.create("d");

		@SuppressWarnings("static-access")
		Option stop = OptionBuilder.hasArg().withArgName("Stop File Name")
				.withLongOpt("stopword")
				.withDescription("Optional. specify file containing stop words ")
				.create("s");

		@SuppressWarnings("static-access")
		Option output = OptionBuilder
				.isRequired()
				.hasArg()
				.withArgName("Output Directory Name")
				.withLongOpt("output")
				.withDescription(
						"specify the output directory for all input xml file")
				.create("o");
        
		@SuppressWarnings("static-access")
		Option help=OptionBuilder.hasArg(false).withLongOpt("help").withDescription("show help").create("h");
		
		options.addOption(stop).addOption(dir).addOption(output).addOption(help);
		try {
			CommandLine commandline = line.parse(options, args);
			String inputDir = "";
			String outputDir = "";
			String stopwordFile = "";
			// parse begin
			if(commandline.hasOption("h")){
				throw new ParseException("for invoking help printer");
			}
			if (commandline.hasOption("s")) {
				stopwordFile = commandline.getOptionValue("s");
			}
			inputDir = commandline.getOptionValue("d");
			outputDir = commandline.getOptionValue("o");

			LocalIndexer indexer = new LocalIndexer(inputDir, outputDir,
					stopwordFile);
			indexer.run();

		} catch (ParseException e) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("LocalIndexer", options);

		}

	}
}
