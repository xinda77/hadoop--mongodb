package mongodbTest;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bson.BSONObject;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;
public class BatcHdfs {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		MongoConfigUtil.setInputURI(conf, "mongodb://182.92.225.106:9980/APPDB.Stock300");  
		MongoConfigUtil.setCreateInputSplits(conf, false);  
		Job job = new Job(conf);
		job.setJarByClass(BatcHdfs.class);
		job.setJobName("BatcHdfs");
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReduce.class);
		job.setInputFormatClass(MongoInputFormat.class); 
		job.setOutputFormatClass(AlphabetOutputFormat.class);
	    Path out = new Path("hdfs://master:9000/out2"); 
	    FileOutputFormat.setOutputPath(job, out);  
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	public static class AlphabetOutputFormat extends MyMultiOutputFormat<Text, Text> {
		@Override
		protected String generateFileNameForKeyValue(Text key, Text value, Configuration conf) {
			String c = key.toString().toLowerCase();
			if (c.length()!=0) {
				return c + ".txt";
			}
			return "other.txt";
		}
	}
}
class MyMapper extends Mapper<Object, BSONObject, Text, Text> {
	@Override
	protected void map(Object key, BSONObject value,Context context)
			throws IOException, InterruptedException {
		Configuration conf=context.getConfiguration();
		  String fileName=MongoConfigUtil.getCollection( MongoConfigUtil.getInputURI(conf)).toString();
		context.write(new Text(fileName), new Text(value.toString()));
    }
}
class MyReduce extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text k, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		for(Text t:values){
			String str=t.toString();
			context.write(k, new Text(str));
		}
	}
}
