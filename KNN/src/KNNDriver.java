import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class KNNDriver {
	public static List<Float> listP;
	public static int k = 3;
	
	public static void main(String[] args) {
		listP = new ArrayList<Float>();
		for(float i=3; i<30; i+=3) {
			listP.add(i);
		}
//		listP.add((float)7);
		
		JobClient my_client = new JobClient();
		// Create a configuration object for the job
		JobConf job_conf = new JobConf(KNNDriver.class);

		// Set a name of the Job
		job_conf.setJobName("Kiem tra cuoi ki");

		// Specify data type of output key and value
		job_conf.setOutputKeyClass(Text.class);
		job_conf.setOutputValueClass(Text.class);

		// Specify names of Mapper and Reducer Class
		job_conf.setMapperClass(KNNMapper.class);
		job_conf.setReducerClass(KNNReducer.class);

		// Specify formats of the data type of Input and output
		job_conf.setInputFormat(TextInputFormat.class);
		job_conf.setOutputFormat(TextOutputFormat.class);

		// Set input and output directories using command line arguments, 
		//arg[0] = name of input directory on HDFS, and arg[1] =  name of output directory to be created to store the output file.
		
//		FileInputFormat.setInputPaths(job_conf, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job_conf, new Path(args[1]));
		FileInputFormat.setInputPaths(job_conf, new Path("input.txt"));
		FileOutputFormat.setOutputPath(job_conf, new Path("output"));
		
		my_client.setConf(job_conf);
		try {
			// Run the job 
			System.out.println("run");
			JobClient.runJob(job_conf);
	    	System.out.println("Done. 10 diem ve cho");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
