package cloudy.topo;

import kafka.productor.KafkaProperties;
import cloudy.bolt.AreaAmtBolt;
import cloudy.bolt.AreaFilterBolt;
import cloudy.bolt.AreaRsltBolt;
import cloudy.spout.OrderBaseSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class AreaAmtTopo {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new OrderBaseSpout(KafkaProperties.Order_topic), 5);
		builder.setBolt("filter", new AreaFilterBolt() , 5).shuffleGrouping("spout") ;
		builder.setBolt("areabolt", new AreaAmtBolt() , 2).fieldsGrouping("filter", new Fields("area_id")) ;
		builder.setBolt("rsltBolt", new AreaRsltBolt(), 1).shuffleGrouping("areabolt");
		
		Config conf = new Config() ;
		conf.setDebug(false);
		conf.setNumWorkers(10);
		if (args.length > 0) {
			try {
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			}
		}else {
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("mytopology", conf, builder.createTopology());
		}
		
		
	}

}
