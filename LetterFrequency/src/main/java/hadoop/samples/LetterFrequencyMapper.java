package hadoop.samples;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class LetterFrequencyMapper extends Mapper<Object, Text, Text, IntWritable> {

	private final IntWritable ONE = new IntWritable(1);
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		for(int i=0;i<value.toString().length();i++){
			context.write(new Text(Character.toString((char)value.charAt(i)).toUpperCase()), ONE);
		}
		
	}	
}
