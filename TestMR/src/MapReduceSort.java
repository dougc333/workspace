import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

//identity mapper and reducer
public class MapReduceSort {

	static class IdentMapper extends Mapper<Object, Text, Text, IntWritable> {
		public void map(Object object, Text t1, Context context) {

			StringTokenizer st = new StringTokenizer(t1.toString());
			while (st.hasMoreTokens()) {
				String token = st.nextToken();
				try {
					context.write(new Text(token), new IntWritable(1));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}
	}

	static class IdentReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text text, IntWritable iw, Context context) {
			try {
				context.write(text, iw);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	// how to print this? should be written to HDFS in FileOutputFormat
	public static void main(String[] args) {

		try {

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
