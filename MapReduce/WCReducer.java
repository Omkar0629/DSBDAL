import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class WCReducer extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable>{
	int mx=0;
	Text mx_c=new Text();
	public void reduce(Text key,Iterator<IntWritable> value,OutputCollector<Text,IntWritable> output,Reporter rep) throws IOException
	{
		int c=0;
		while(value.hasNext())
		{
			IntWritable i=value.next();
			c+=i.get();
		}
		boolean z=false;
		if(c>mx)
		{
			mx=c;
			mx_c.set(key);
			z=true;
		}
		if(mx>10 && z)
		{
		output.collect(mx_c,new IntWritable(mx));
		}
	}

}
