import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TestThrift extends Configured implements Tool {

	public static void main(String[] args) {

		try {
			int returnCode = ToolRunner.run(new TestThrift(), args);
			System.out.println(returnCode);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Job job = new Job();
		job.setJarByClass(TestThrift.class);
		job.setJobName("TestThrift");
		job.setNumReduceTasks(0);

		return 0;
	}
}
