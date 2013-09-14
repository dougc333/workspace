import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Sort1 extends Configured implements Tool {
	private static Job job;
	private static Configuration conf;

	static class Sort1Mapper extends Mapper<Object, Text, Text, IntWritable> {

		IntWritable intWritable = new IntWritable();
		Text tOutput = new Text();

		public void map(Object obj, Text text, Context context)
				throws IOException, InterruptedException {
			StringTokenizer st = new StringTokenizer(text.getBytes().toString());
			while (st.hasMoreTokens()) {
				String token = st.nextToken();
				System.out.println(token);
				context.write(tOutput, intWritable);
			}
		}
	}

	static class Sort1Reducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		int sum = 0;

		public void reduce(Text text, IntWritable iw, Context context)
				throws IOException, InterruptedException {
			sum = sum + iw.get();
			context.write(text, new IntWritable(sum));
		}

	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Sort1(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Configuration getConf() {
		return conf;
	}

	public void setConf() throws IOException {
		Configuration conf = new Configuration();
		conf.addResource("hdfs-site.xml");
		conf.addResource("core-site.xml");
		job = new Job(conf);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		setConf();
		job.setJarByClass(WordCount.class);
		job.setMapperClass(Sort1Mapper.class);
		job.setReducerClass(Sort1Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path("/user/dc/sort"));
		FileOutputFormat.setOutputPath(job,
				new Path("/user/dc/sort/testoutput"));

		int returnCode = job.waitForCompletion(true) ? 0 : 1;
		return returnCode;
	}
}
