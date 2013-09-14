import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.ToolRunner;

public class TestSequenceFiles {

	static class TSFMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object obj, Text text, Context context) {
			// we are just converting files, not lines
			context.write(obj, text);
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource("hdfs-site.xml");

		Job jobConf = new Job();
		jobConf.setJobName("TestSequenceFiles");
		jobConf.setJarByClass(TestSequenceFiles.class);
		jobConf.setNumReduceTasks(0);
		jobConf.setMapperClass(new IdentityMapper<Object, Text>());
		jobConf.setMapOutputKeyClass(LongWritable.class);
		jobConf.setMapOutputValueClass(Text.class);
		jobConf.setOutputKeyClass(LongWritable.class);
		jobConf.setOutputValueClass(Text.class);
		jobConf.setInputFormatClass(TextInputFormat.class);
		jobConf.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.setInputPaths(jobConf,
				new Path("/user/dc/sequencefile"));
		FileOutputFormat.setOutPaths(jobConf, new Path(
				"/user/dc/sequencefile/output"));
		System.exit(jobConf.waitForCompletion(true) ? 0 : 1);

	}

	public static void main(String[] args) {
		try {
			int returnCode = ToolRunner.run(tool, args);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
