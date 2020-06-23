import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class KNNMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private Text keyP = new Text();

	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		System.out.println("mapper");

		StringTokenizer itr = new StringTokenizer(value.toString());
	      while (itr.hasMoreTokens()) {
	    	String s = itr.nextToken();
	    	String[] input = s.split(",");
	    	float point = Float.parseFloat(input[0]);
	    	
	    	for(int i=0; i<KNNDriver.listP.size(); i++) {
	    		float p = KNNDriver.listP.get(i);
//	    		System.out.println(p);
		    	float distance = (float)Math.sqrt((point - p)*(point - p));

		    	keyP.set(Float.toString(p));
		        Text val = new Text();
		        val.set(distance + " " + input[1]);
		        output.collect(keyP, val);
//		        System.out.println(keyP.toString() + " " + val.toString());
	    	}
	      }
	}
}
