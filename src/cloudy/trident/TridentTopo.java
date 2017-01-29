package cloudy.trident;

import hbase.state.HBaseAggregateState;
import hbase.state.TridentConfig;
import kafka.productor.KafkaProperties;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.FirstN;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.StateFactory;
import storm.trident.testing.MemoryMapState;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import cloudy.trident.tools.OrderAmtSplit;
import cloudy.trident.tools.OrderNumSplit;
import cloudy.trident.tools.Split;
import cloudy.trident.tools.SplitBy;

public class TridentTopo {

	public static StormTopology builder(LocalDRPC drpc)
	{
	    TridentConfig tridentConfig = new TridentConfig("state");
	    StateFactory state = HBaseAggregateState.transactional(tridentConfig);
		
		BrokerHosts zkHosts = new ZkHosts(KafkaProperties.zkConnect);
		String topic = "track";
		TridentKafkaConfig config = new TridentKafkaConfig(zkHosts, topic);
		config.forceFromStart = false; //测试时用true，上线时必须改为false
		config.scheme = new SchemeAsMultiScheme(new StringScheme());
		config.fetchSizeBytes = 100 ;//batch size
		
		TransactionalTridentKafkaSpout spout  = new TransactionalTridentKafkaSpout(config) ;
		
		TridentTopology topology = new TridentTopology() ;
		//销售额
		TridentState amtState = topology.newStream("spout", spout)
		.parallelismHint(3)
		.each(new Fields(StringScheme.STRING_SCHEME_KEY),new OrderAmtSplit("\\t"), new Fields("order_id","order_amt","create_date","province_id","cf"))
		.shuffle()
		.groupBy(new Fields("create_date","cf","province_id"))
		.persistentAggregate(state, new Fields("order_amt"), new Sum(), new Fields("sum_amt"));
//		.persistentAggregate(new MemoryMapState.Factory(), new Fields("order_amt"), new Sum(), new Fields("sum_amt"));
		
		topology.newDRPCStream("getOrderAmt", drpc)
		.each(new Fields("args"), new Split(" "), new Fields("arg"))
		.each(new Fields("arg"), new SplitBy("\\:"), new Fields("create_date","cf","province_id"))
		.groupBy(new Fields("create_date","cf","province_id"))
		.stateQuery(amtState, new Fields("create_date","cf","province_id"), new MapGet(), new Fields("sum_amt"))
		.each(new Fields("sum_amt"),new FilterNull())
		.applyAssembly(new FirstN(5, "sum_amt", true))
		;
		
		//订单数
		TridentState orderState = topology.newStream("orderSpout", spout)
		.parallelismHint(3)
		.each(new Fields(StringScheme.STRING_SCHEME_KEY),new OrderNumSplit("\\t"), new Fields("order_id","order_amt","create_date","province_id","cf"))
		.shuffle()
		.groupBy(new Fields("create_date","cf","province_id"))
		.persistentAggregate(state, new Fields("order_id"), new Count(), new Fields("order_num"));
//		.persistentAggregate(new MemoryMapState.Factory(), new Fields("order_id"), new Count(), new Fields("order_num"));
		
		topology.newDRPCStream("getOrderNum", drpc)
		.each(new Fields("args"), new Split(" "), new Fields("arg"))
		.each(new Fields("arg"), new SplitBy("\\:"), new Fields("create_date","cf","province_id"))
		.groupBy(new Fields("create_date","cf","province_id"))
		.stateQuery(orderState, new Fields("create_date","cf","province_id"), new MapGet(), new Fields("order_num"))
		.each(new Fields("order_num"),new FilterNull())
//		.applyAssembly(new FirstN(5, "order_num", true))
		;
		return topology.build() ;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		LocalDRPC drpc = new LocalDRPC();
		Config conf = new Config() ;
		conf.setNumWorkers(5);
		conf.setDebug(false);
		if (args.length > 0) {
			try {
				StormSubmitter.submitTopology(args[0], conf, builder(null));
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			}
		}else {
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("mytopology", conf, builder(drpc));
		}
//		Utils.sleep(60000);
//		while (true) {
//			System.err.println("销售额："+drpc.execute("getOrderAmt", "2014-09-13:cf:amt_3 2014-09-13:cf:amt_2 2014-09-13:cf:amt_1 2014-09-13:cf:amt_7 2014-09-13:cf:amt_6 2014-09-13:cf:amt_5 2014-09-13:cf:amt_4 2014-09-13:cf:amt_8")) ;
//			Utils.sleep(5000);
//		}
		/**
		 * [["2014-08-19:1 2014-08-19:2 2014-08-19:3 2014-08-19:4 2014-08-19:5","2014-08-19:1","2014-08-19","1",821.9000000000001],
		 * ["2014-08-19:1 2014-08-19:2 2014-08-19:3 2014-08-19:4 2014-08-19:5","2014-08-19:2","2014-08-19","2",631.3000000000001],
		 * ["2014-08-19:1 2014-08-19:2 2014-08-19:3 2014-08-19:4 2014-08-19:5","2014-08-19:3","2014-08-19","3",240.7],
		 * ["2014-08-19:1 2014-08-19:2 2014-08-19:3 2014-08-19:4 2014-08-19:5","2014-08-19:4","2014-08-19","4",340.4],
		 * ["2014-08-19:1 2014-08-19:2 2014-08-19:3 2014-08-19:4 2014-08-19:5","2014-08-19:5","2014-08-19","5",460.8]]
		 */
		
		
	}

}
