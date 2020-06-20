import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class WordCount1Reducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	
	public float getValue(String s) {
		return Float.parseFloat(s.split(" ")[0]);
	}
	
	public String getClass(String s) {
		return s.split(" ")[1];
	}	
	
	public int indexMax(List<String> a) {
		int index=0;
		float MAX = getValue(a.get(0));
		for(int i=1; i<a.size(); i++) {
			float val = getValue(a.get(i));
			if(val > MAX) {
				MAX = val;
				index = i;
			}
		}
		return index;
	}
	
	int countClass(List<String> a, String s) {
		int count = 0;
		for(int i=0; i<a.size(); i++) {
			if(a.get(i).equals(s)) {
				count++;
			}
		}
		return count;
	}
	public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
		Text key = t_key;
		System.out.println("reduce");
		List<String> k_min = new ArrayList<String>();
		for(int i=0; i<WordCount1Driver.k; i++) k_min.add("999999999 ");
		
		while (values.hasNext()) {
			String value = values.next().toString();
//			System.out.println(value.toString());

			int indexMax = indexMax(k_min);
			if(getValue(k_min.get(indexMax)) > getValue(value) ) {
				k_min.set(indexMax, value.toString());
			}
			
		}
//		for(int i=0; i<k_min.size(); i++) System.out.println(k_min.get(i));
		
		String classes = getClass(k_min.get(0));
		int MAX = countClass(k_min, classes);
		
		for(int i=1; i<k_min.size(); i++) {
			if(!getClass(k_min.get(i)).equals(classes)) {
				continue;
			}
			int count = countClass(k_min, getClass(k_min.get(i)));
			if(MAX < count) {
				MAX = count;
				classes = getClass(k_min.get(i));
			}
		}
		
		output.collect(key, new Text(classes));
	}
}
