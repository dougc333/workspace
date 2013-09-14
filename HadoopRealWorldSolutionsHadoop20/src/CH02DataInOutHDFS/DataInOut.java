package CH02DataInOutHDFS;



import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class DataInOut {

	static class TestTool extends Configured implements Tool{

		@Override
		public int run(String[] arg0) throws Exception {
			// TODO Auto-generated method stub
			return 0;
		}
	}
	
	
	public static void main(String []args){
		try{
		
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}
	
}
