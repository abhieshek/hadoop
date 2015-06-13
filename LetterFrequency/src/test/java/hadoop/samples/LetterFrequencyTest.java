package hadoop.samples;
import static org.junit.Assert.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class LetterFrequencyTest 
{

	MapDriver<Object, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

	@Before
	public void setUp() {
		LetterFrequencyMapper mapper = new LetterFrequencyMapper();
		LetterFrequencyReducer reducer = new LetterFrequencyReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}


	@Test
	public void testReducer()  throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		reduceDriver.withInput(new Text("T"), values);
		reduceDriver.withOutput(new Text("T"), new IntWritable(2));
		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce()  throws IOException {
		mapReduceDriver.withInput(new LongWritable(), new Text(
				"This is sample data"));
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		List<Pair<Text, IntWritable>> output = mapReduceDriver.run();
		for (Pair<Text, IntWritable> p : output) {
			if(p.getFirst().toString().equalsIgnoreCase("T"))
				assertEquals(2, p.getSecond().get());
		}
	}
}
