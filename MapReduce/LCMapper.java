import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

//extras
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.ParseException;

public class LCMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable> {
	public void map(LongWritable key,Text value,OutputCollector<Text,IntWritable>output,Reporter rep) throws IOException
	{
		String [] values=value.toString().split(",");
		
		String user=values[1];
		String starttime=values[5];
		String endtime=values[7];
		
		long  startTimeStamp=0;
		long endTimeStamp=0;
		
		SimpleDateFormat format=new SimpleDateFormat("d/M/yyyy H:mm");
		try
		{ Date startdate=format.parse(starttime);
		Date enddate=format.parse(endtime);
		
		startTimeStamp=startdate.getTime();
		endTimeStamp=enddate.getTime();
			
		}
		catch(ParseException e)
		{
			
		}
		
		int duration=(int) (endTimeStamp-startTimeStamp);
		
		output.collect(new Text(user),new IntWritable(duration));
		
		
		
		
	}

}
