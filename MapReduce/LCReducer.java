import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class LCReducer extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable>  {
	
	public void reduce(Text key,Iterator<IntWritable>values,OutputCollector<Text,IntWritable>output,Reporter rep) throws IOException
	{
		int count=0;
		
		while(values.hasNext())
		{
			IntWritable value=values.next();
			count+=value.get();
		}
		
		output.collect(key, new IntWritable(count));
	}

}
