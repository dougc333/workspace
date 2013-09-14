import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;





public class TestMiniDFSCluster {

	public static void main(String []args) throws Exception{
		Configuration conf = new Configuration();
		//add current core-site.xml or create a new one? 
	
		MiniDFSCluster.Builder builder= new MiniDFSCluster.Builder(conf);
		System.out.println("builder defaults:"+builder.toString());
		//namenode at port 0!
		MiniDFSCluster clust = new MiniDFSCluster();
		clust.waitActive();
		//can you add and remove data from the file system? 
		DistributedFileSystem dfs = clust.getFileSystem();
		DFSClient client = dfs.getClient();
		//how to load flume/hdfs into this cluster? 
		//how do the unit tests do this? 
	}
	
}
