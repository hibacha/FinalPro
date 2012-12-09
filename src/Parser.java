package neu;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Parser {

	private static String CURL_CMD = "curl --silent http://en.wikipedia.org/w/api.php?action=query&prop=info&pageids=307&inprop=url";
	private static String CURL_CMD_PREFIX = "curl --silent http://en.wikipedia.org/w/api.php?action=query&prop=info&pageids=";
	private static String CURL_CMD_SUFFIX = "&inprop=url";
	private static String CURL_CMD2 = "ifconfig";
	private static String LINCOLN_RESULT = "/Users/zhouyf/Stack/cs6240/final/output/lincoln/part-r-00000";
	private static String WORDS_RESULT = "/Users/zhouyf/Stack/cs6240/final/output/stat_word01/part-r-00000";

	public static ArrayList<String> read(String filePath, int index, int counter)
			throws IOException {
		File file = new File(filePath);
		BufferedReader bf = new BufferedReader(new FileReader(file));
		ArrayList<String> rst = new ArrayList<String>();
		String line = null;
		while ((line = bf.readLine()) != null) {

			if (!(counter > 0))
				break;
			String[] fields = line.split("\t");
			try {
				Long.parseLong(fields[index]);
				rst.add(fields[index-1]+"\t"+fields[index]);
				// System.out.println(fields[index]);
			} catch (NumberFormatException exception) {

			}
			counter--;
		}

		bf.close();
		return rst;

	}

	public static ArrayList<String> reverse(ArrayList<String> array) {
		ArrayList<String> rst = new ArrayList<String>(array.size());
		while (array.size() > 0) {
			rst.add(array.get(array.size() - 1));
			array.remove(array.size() - 1);
		}
		return rst;
	}

	public static void main(String args[]) throws IOException {
		//ArrayList<String> rst = read(WORDS_RESULT, 2, 1500);
		 ArrayList<String> rst = read(LINCOLN_RESULT, 2, 500);
		Pattern pattern = Pattern.compile("title=&quot;(.*?)&quot;");

		rst = reverse(rst);
//		File output = new File(
//				"/Users/zhouyf/Stack/cs6240/final/output/stat_word01/analysis.txt");
		File output = new File(
				"/Users/zhouyf/Stack/cs6240/final/output/lincoln/analysis.txt");
		BufferedWriter bw = new BufferedWriter(new FileWriter(output, false));
		while (rst.size() > 0) {
			try {
				String combined= rst.get(rst.size() - 1);
				String id = combined.split("\t")[1];
				Process p = Runtime.getRuntime().exec(
						"curl http://en.wikipedia.org/w/api.php?action=query&prop=info&pageids="
								+ id + "&inprop=url");
				rst.remove(rst.size() - 1);

				p.waitFor();
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(p.getInputStream()));
				String line = null;
				while ((line = reader.readLine()) != null) {
					Matcher matcher = pattern.matcher(line);
					if (matcher.find()) {
						String title = matcher.group(1).toLowerCase();
						//if (!title.startsWith("wikipedia")) {
							System.out.println(combined + "\t" + title);
							bw.write(combined + "\t\t" + matcher.group(1) + "\n");
							bw.flush();
						//}
						// System.out.println(matcher.group(0));
						// System.out.println(line);
					}

				}

			} catch (IOException e1) {
			} catch (InterruptedException e2) {
			}
		}
		bw.close();
	}
}