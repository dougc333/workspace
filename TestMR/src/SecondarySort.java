import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SecondarySort extends Configured implements Tool {
	private static Job job;

	public class CompositeKey implements WritableComparable {
		private Text first = null;
		private Text second = null;

		public CompositeKey() {

		}

		public CompositeKey(Text first, Text second) {
			this.first = first;
			this.second = second;
		}

		public void write(DataOutput d) throws IOException {
			first.write(d);
			second.write(d);
		}

		public Text getFirst() {
			return first;
		}

		public void setFirst(Text first) {
			this.first = first;
		}

		public Text getSecond() {
			return second;
		}

		public void setSecond(Text second) {
			this.second = second;
		}

		public void readFields(DataInput di) throws IOException {
			if (first == null) {
				first = new Text();
			}
			if (second == null) {
				second = new Text();
			}
			first.readFields(di);
			second.readFields(di);
		}

		@Override
		public int compareTo(Object o) {
			// TODO Auto-generated method stub
			return 0;
		}

	}// end CompositeKey

	public class PageViewMapper extends
			Mapper<Object, Text, CompositeKey, Text> {
		private CompositeKey compositeKey = new CompositeKey();
		private Text first = new Text();
		private Text second = new Text();
		private Text outputValue = new Text();

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\t");
			if (tokens.length > 3) {
				String page = tokens[2];
				String ip = tokens[0];
				first.set(page);
				second.set(ip);
				compositeKey.setFirst(first);
				compositeKey.setSecond(second);
				outputValue.set(ip);
				context.write(compositeKey, outputValue);
			}
		}
	}

	public class PageViewReducer extends
			Reducer<CompositeKey, Text, Text, LongWritable> {
		private LongWritable pageViews = new LongWritable();

		@Override
		protected void reduce(CompositeKey key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			String lastIp = null;
			long pages = 0;
			for (Text t : values) {
				String ip = t.toString();
				if (lastIp == null) {
					lastIp = ip;
					pages++;
				} else if (!lastIp.equals(ip)) {
					lastIp = ip;
					pages++;
				} else if (lastIp.compareTo(ip) > 0) {
					throw new IOException("secondary sort failed");
				}
			}
			pageViews.set(pages);
			context.write(key.getFirst(), pageViews);
		}
	}

	static class CompositeKeyParitioner extends
			Partitioner<CompositeKey, Writable> {

		@Override
		public int getPartition(CompositeKey key, Writable value,
				int numParition) {
			return (key.getFirst().hashCode() & 0x7FFFFFFF) % numParition;
		}
	}

	static class GroupComparator extends WritableComparator {
		public GroupComparator() {
			super(CompositeKey.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			CompositeKey lhs = (CompositeKey) a;
			CompositeKey rhs = (CompositeKey) b;
			return lhs.getFirst().compareTo(rhs.getFirst());
		}
	}

	static class SortComparator extends WritableComparator {
		public SortComparator() {
			super(CompositeKey.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			CompositeKey lhs = (CompositeKey) a;
			CompositeKey rhs = (CompositeKey) b;
			int cmp = lhs.getFirst().compareTo(rhs.getFirst());
			if (cmp != 0) {
				return cmp;
			}
			return lhs.getSecond().compareTo(rhs.getSecond());
		}
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new SecondarySort(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public int run(String[] arg0) throws Exception {

		Path inputPath = new Path(arg0[0]);
		Path outputPath = new Path(arg0[1]);

		Configuration conf = getConf();
		Job weblogJob = new Job(conf);
		weblogJob.setJobName("PageViews");
		weblogJob.setJarByClass(getClass());
		weblogJob.setMapperClass(PageViewMapper.class);
		weblogJob.setMapOutputKeyClass(CompositeKey.class);
		weblogJob.setMapOutputValueClass(Text.class);

		weblogJob.setPartitionerClass(CompositeKeyParitioner.class);
		weblogJob.setGroupingComparatorClass(GroupComparator.class);
		weblogJob.setSortComparatorClass(SortComparator.class);

		weblogJob.setReducerClass(PageViewReducer.class);
		weblogJob.setOutputKeyClass(Text.class);
		weblogJob.setOutputValueClass(Text.class);
		weblogJob.setInputFormatClass(TextInputFormat.class);
		weblogJob.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(weblogJob, inputPath);
		FileOutputFormat.setOutputPath(weblogJob, outputPath);

		if (weblogJob.waitForCompletion(true)) {
			return 0;
		}
		return 1;

		int returnCode = job.waitForCompletion(true) ? 0 : 1;
		return returnCode;
	}
}
