import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Scanner;
import java.lang.Math;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class ClusterCenter{
	public static class Map extends Mapper<LongWritable, Text, IntWritable, Text>{
		private ArrayList<String> clusterCenters = new ArrayList<String>();

		@Override
		protected void setup(Context context) throws IOException,InterruptedException{
			super.setup(context);
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataInputStream fsi = null;
			//get prior cluster centers' information
			fsi = fs.open(new Path(context.getConfiguration().get("clusterPath")));
			Scanner scan = new Scanner(fsi);

			while (scan.hasNext()){
				String str = scan.nextLine();
				clusterCenters.add(str);
			}

			fsi.close();
		}

		@Override
		public void map(LongWritable key, Text line, Context context)throws IOException, InterruptedException{
			//get the nearest point's id of the line
			System.out.println(line);
			IntWritable id = new IntWritable(getNearest(line));
			System.out.println(id);
			context.write(id, line);
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

	public static class Reduce extends Reducer<IntWritable, Text, NullWritable, Text>{
		public void reduce(IntWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
			//recaculate cluster centers
			double sum_x = 0;
			double sum_y = 0;
			int id = key.get();
			int count = 0;

			for (Text each : value){
				count +=1 ;
				String[] nums = each.toString().split(",");
				double x = Double.parseDouble(nums[0]);
				double y = Double.parseDouble(nums[1]);

				sum_x = sum_x + x;
				sum_y = sum_y + y;
			}

			double avg_x = sum_x/count;
			double avg_y = sum_y/count;

			String newCenter = String.valueOf(avg_x)+","+String.valueOf(avg_y)+","+String.valueOf(id);
			Text content = new Text();
			content.set(newCenter);

			context.write(NullWritable.get(), content);
		}
	}
}
