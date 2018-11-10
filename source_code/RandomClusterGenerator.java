import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Random;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

public final class RandomClusterGenerator{
	private int k;
	private Path inputPath;
	private Configuration conf;
	private FileSystem fs;

	public RandomClusterGenerator(int k, String input, Configuration conf) throws IOException{
		this.k = k;
		this.conf = conf;
		inputPath = new Path(input);
		fs = FileSystem.get(URI.create(input), conf);
	}

	public void initialize(String targetPath){
		FSDataInputStream fsi = null;
		FSDataOutputStream fso = null;

		try {
			//Create file to store cluster centers' information
			fsi = fs.open(inputPath);
			fso = fs.create(new Path(targetPath));

			//Read raw data and write into new file
			Scanner scan = new Scanner(fsi);
			String str = new String();
			ArrayList<String> strList = new ArrayList<String>();

			while (scan.hasNext()){
				str = scan.nextLine();
				strList = makeDecision(strList, str);
			}

			String totalStr = "";
			for (String each : strList){
				totalStr = totalStr + each + "\n";
			}

			fsi.close();
			fso.write(totalStr.getBytes());
			fso.flush();
			fso.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				fsi.close();
			} catch (IOException e) {
				e.printStackTrace();
			}	
		}
	}

	private int randomChoose(int k){
		Random random = new Random();
		if(random.nextInt(k + 1) == 0)
			//choose one center to replace another randomly
			return new Random().nextInt(k);
		else
			//do nothing
			return -1;
	}

	private ArrayList<String> makeDecision(ArrayList<String> list, String line){
		if (list.size() < k)
			list.add(line + "," + String.valueOf(list.size()));
		else if (list.size() >= k){
			int choice = randomChoose(k);
			if (choice != -1)
				list.set(choice, line + "," + String.valueOf(choice));
		}
		return list;
	}
}
