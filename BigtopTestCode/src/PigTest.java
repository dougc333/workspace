

import org.apache.pig.*;
import java.io.*;

import org.apache.hadoop.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

//for 1k rows, 2x, prob gets longer as we go longer rows
//hdfs elapsed time:476
//local file system elapsed time:198
//space diff.
//-rw-r--r--   1 dc dc     291890 2013-07-06 17:04 /user/dc/testpig.txt
//-rw-r--r--   1 dc dc     290890 2013-07-06 17:04 /user/dc/testMe
//300kb, 500ms
//3mb 5000ms, 5s
//30mb 50s
//300mb 500s, 8min
//3gb 80min, 
//30gb 800min, starts to get bad...
public class PigTest {
	private static final Integer NUM_ROWS=1000;
	
	
	public static void main(String []args)throws Exception{
		
		PigServer pS = new PigServer(ExecType.LOCAL);
		
		//write to hdfs
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/lib/hadoop/etc/hadoop/hdfs-site.xml"));
		conf.addResource(new Path("/usr/lib/hadoop/etc/hadoop/core-site.xml"));
		Path testMe = new Path("/user/dc/testMe");
		FileSystem fs = FileSystem.get(conf);
		
		if(fs.exists(testMe)){
			fs.delete(testMe);
		}
		long hdfsStartTime = System.currentTimeMillis();
		if(!fs.exists(testMe)){
	        fs.mkdirs(new Path("/user/dc/testing"));
	        FSDataOutputStream fdos=fs.create(testMe);
	        for(int i=0;i<NUM_ROWS;i++){
	        fdos.writeBytes(i+"\t"+"column1"+"\t"+"column2"+"\t"+"column3"
					+"\t"+"column4"+"\t"+"column5"+"\t"+"column6"
					+"\t"+"column7"+"\t"+"column8"+"\t"+"column9"
					+"\t"+"column10"+"\t"+"column11"+"\t"+"column12"
					+"\t"+"column13"+"\t"+"column14"+"\t"+"column15"
					+"\t"+"column16"+"\t"+"column17"+"\t"+"column18"
					+"\t"+"column19"+"\t"+"column20"+"\t"+"column21"
					+"\t"+"column22"+"\t"+"column23"+"\t"+"column24"
					+"\t"+"column25"+"\t"+"column26"+"\t"+"column27"
					+"\t"+"column28"+"\t"+"column29"+"\t"+"column30"
					+"\t"+"column31"+"\t"+"column32"+"\t"+"column33");
	        }
	        fdos.flush();
	        fdos.close();
	        
		}
		long hdfsEndTime=System.currentTimeMillis();
		//move file in
		long startLocalFileSystem=System.currentTimeMillis();
		BufferedWriter bw = new BufferedWriter(new FileWriter("testpig.txt"));
		for(int i=0;i<NUM_ROWS;i++){
			bw.write(i+"\t"+"column1"+"\t"+"column2"+"\t"+"column3"
					+"\t"+"column4"+"\t"+"column5"+"\t"+"column6"
					+"\t"+"column7"+"\t"+"column8"+"\t"+"column9"
					+"\t"+"column10"+"\t"+"column11"+"\t"+"column12"
					+"\t"+"column13"+"\t"+"column14"+"\t"+"column15"
					+"\t"+"column16"+"\t"+"column17"+"\t"+"column18"
					+"\t"+"column19"+"\t"+"column20"+"\t"+"column21"
					+"\t"+"column22"+"\t"+"column23"+"\t"+"column24"
					+"\t"+"column25"+"\t"+"column26"+"\t"+"column27"
					+"\t"+"column28"+"\t"+"column29"+"\t"+"column30"
					+"\t"+"column31"+"\t"+"column32"+"\t"+"column33");
			bw.newLine();
		}
		bw.close();
		fs.copyFromLocalFile(new Path("testpig.txt"), new Path("/user/dc"));
		fs.close();
		long endLocalFileSystem=System.currentTimeMillis();
		
		System.out.println("hdfs elapsed time:"+(hdfsEndTime-hdfsStartTime));
		
		System.out.println("local file system elapsed time:"+(endLocalFileSystem-startLocalFileSystem));
		//measure speed? vs space? 
		
		
		
	}
	
}
