import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Scanner;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KMeansCluster{
	public static class Map extends Mapper<LongWritable, Text, Text, NullWritable>{
		private ArrayList<String> clusterCenters = new ArrayList<String>();

		protected void setup(Context context) throws IOException, InterruptedException{
			super.setup(context);
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataInputStream fsi = null;
			//get the last cluster centers' information
			fsi = fs.open(new Path(context.getConfiguration().get("clusterPath")));
			Scanner scan = new Scanner(fsi);

			while (scan.hasNext()){
				String str = scan.nextLine();
				clusterCenters.add(str);
			}

			fsi.close();
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			//get the nearest point's id of the line
			int id = getNearest(value);
			String content = value.toString() + "," + String.valueOf(id);
			Text word = new Text();
			word.set(content);
			context.write(word, NullWritable.get());
		}

		private int getNearest(Text line){
			String[] value = line.toString().split(",");
			double x_target = Double.parseDouble(value[0]);
			double y_target = Double.parseDouble(value[1]);

			int id = -1;
			double distance = 1000000000;

			for (String each : clusterCenters){
				String[] nums = each.split(",");
				double x = Double.parseDouble(nums[0]);
				double y = Double.parseDouble(nums[1]);

				double tmpDistance = Math.pow(Math.pow(x_target-x, 2) + Math.pow(y_target-y, 2), 0.5);
				if (tmpDistance < distance){
					id = Integer.parseInt(nums[2]);
					distance = tmpDistance;
				}
			}

			return id;
		}
	}
}
