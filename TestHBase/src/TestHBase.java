//import javax.security.auth.login.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class TestHBase {

	public static void main(String[] args) {
		try {
			// import the jars under hadoop common. not just the
			// hadoop-2.0.5-alpha
			org.apache.hadoop.conf.Configuration c = HBaseConfiguration
					.create();
			HBaseAdmin admin = new HBaseAdmin(c);
			HTableDescriptor desc = new HTableDescriptor("t2");
			HColumnDescriptor col = new HColumnDescriptor("colfam");
			desc.addFamily(col);
			admin.createTable(desc);
			if (!admin.isTableAvailable("t2")) {
				System.out
						.println("error in creating table, check configuration setup");
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
