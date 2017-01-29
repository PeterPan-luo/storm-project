package hbase.state;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;

public class HTableConnector implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Configuration configuration;
	protected HTableInterface table;
	private String tableName;
	
	HConnection hTablePool = null;
	public HTableConnector(TupleTableConfig conf) throws Exception
	{
//		this.tableName = conf.getTableName();
//		this.configuration = HBaseConfiguration.create();
//		
//		String filePathString = "hbase-site.xml" ;
//		Path path = new Path(filePathString) ;
//		this.configuration.addResource(path);
//		this.table = new HTable(this.configuration,this.tableName);
		
		configuration = new Configuration();
		String zk_list = "192.168.1.107,192.168.1.108" ;
		configuration.set("hbase.zookeeper.quorum", zk_list);
		try {
			hTablePool = HConnectionManager.createConnection(configuration) ;
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.tableName = conf.getTableName();
		this.table = hTablePool.getTable(this.tableName) ;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	public HTableInterface getTable() {
		return table;
	}

	public void setTable(HTableInterface table) {
		this.table = table;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public void close()
	{
		try {
			this.table.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
