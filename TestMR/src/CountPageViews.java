import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CountPageViews {

	public static class CompositeKey implements WritableComparable {

		private Text first = null;
		private Text second = null;

		public CompositeKey() {

		}

		public CompositeKey(Text first, Text second) {
			this.first = first;
			this.second = second;
		}

		// ...getters and setters

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

		public int compareTo(Object obj) {
			CompositeKey other = (CompositeKey) obj;
			int cmp = first.compareTo(other.getFirst());
			if (cmp != 0) {
				return cmp;
			}
			return second.compareTo(other.getSecond());
		}

		@Override
		public boolean equals(Object obj) {
			CompositeKey other = (CompositeKey) obj;
			return first.equals(other.getFirst());
		}

		@Override
		public int hashCode() {
			return first.hashCode();
		}
	}

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
			Path inputPath = new Path(args[0]);
			Path outputPath = new Path(args[1]);

			Configuration conf = new Configuration();
			conf.addResource("core-site.xml");
			conf.addResource("hdfs-site.xml");
			conf.addResource("mapred-site.xml");

			Job weblogJob = new Job(conf);
			weblogJob.setJobName("PageViews");
			weblogJob.setJarByClass(CountPageViews.class);
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

			System.exit(weblogJob.waitForCompletion(true) ? 0 : 1);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
