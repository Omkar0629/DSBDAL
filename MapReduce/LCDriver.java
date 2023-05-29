import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class LCDriver extends Configured implements Tool {

	public int run(String args[])throws IOException
	{
		if(args.length<2)
		{
			System.out.println("Invalid input paramters passed");
			return -1;
		}
		
		JobConf conf=new JobConf(LCDriver.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		conf.setMapperClass(LCMapper.class);
		conf.setReducerClass(LCReducer.class);
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		JobClient.runJob(conf);

		return 0;
	}
	
	public static void main(String args[])throws Exception
	{
		int exitcode=ToolRunner.run(new LCDriver(),args);
		System.out.println(exitcode);
		
				
	}
}
