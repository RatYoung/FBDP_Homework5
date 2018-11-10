import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMeans{
	private int k;
	private int iterationNum;
	private Configuration conf;
	private String inputPath;
	private String outputPath;

	public KMeans(int k, int iterationNum, Configuration conf, String inputPath, String outputPath){
		this.k = k;
		this.iterationNum = iterationNum;
		this.conf = conf;
		this.inputPath = inputPath;
		this.outputPath = outputPath;
	}

	public void clusterCenterJob() throws IOException, InterruptedException, ClassNotFoundException{
		for (int i = 0; i < iterationNum; i++){
			Job clusterCenterJob = new Job();
			clusterCenterJob .setJobName("clusterCenterJob" + i);
			clusterCenterJob .setJarByClass(ClusterCenter.class);
			
			clusterCenterJob.getConfiguration().set("clusterPath", outputPath + "/cluster-" + i +"/part-r-00000/");

			clusterCenterJob.setMapperClass(ClusterCenter.Map.class);
			clusterCenterJob.setMapOutputKeyClass(IntWritable.class);
			clusterCenterJob.setMapOutputValueClass(Text.class);

			clusterCenterJob.setReducerClass(ClusterCenter.Reduce .class);
			clusterCenterJob.setOutputKeyClass(NullWritable.class);
			clusterCenterJob.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(clusterCenterJob, new Path(inputPath));
			FileOutputFormat.setOutputPath(clusterCenterJob, new Path(outputPath + "/cluster-" + (i + 1) +"/"));
			
			clusterCenterJob.waitForCompletion(true);
			System.out.println("finished!");
		}
	}

	public void kMeansClusterJob() throws IOException, InterruptedException, ClassNotFoundException{
		Job kMeansClusterJob = new Job();
		kMeansClusterJob.setJobName("KMeansClusterJob");
		kMeansClusterJob.setJarByClass(KMeansCluster.class);
		
		kMeansClusterJob.getConfiguration().set("clusterPath", outputPath + "/cluster-" + (iterationNum - 1) +"/part-r-00000/");

		kMeansClusterJob.setMapperClass(KMeansCluster.Map.class);
		kMeansClusterJob.setMapOutputKeyClass(NullWritable.class);
		kMeansClusterJob.setMapOutputValueClass(Text.class);

		kMeansClusterJob.setNumReduceTasks(0);

		FileInputFormat.addInputPath(kMeansClusterJob, new Path(inputPath));
		FileOutputFormat.setOutputPath(kMeansClusterJob, new Path(outputPath + "/clusteredInstances" + "/"));
		
		kMeansClusterJob.waitForCompletion(true);
		System.out.println("finished!");
	}

	public void generateInitialCluster() throws IOException{
		RandomClusterGenerator generator = new RandomClusterGenerator(k, inputPath, conf);;
		generator.initialize(outputPath + "/cluster-0/part-r-00000/");
	}

	public static void main(String args[]) throws Exception{
		Configuration conf = new Configuration();
		int k = Integer.parseInt(args[0]);
		int iterationNum = Integer.parseInt(args[1]);
		String inputPath = args[2];
		String outputPath = args[3];
		KMeans km = new KMeans(k, iterationNum, conf, inputPath + "/Instance.txt/", outputPath);
		km.generateInitialCluster();
		System.out.println("Cluster Centers Initialized");
		km.clusterCenterJob();
		km.kMeansClusterJob();
	}
}
