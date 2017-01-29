package test;

import kafka.productor.KafkaProperties;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import cloudy.bolt.AreaAmtBolt;
import cloudy.bolt.AreaFilterBolt;
import cloudy.bolt.AreaRsltBolt;
import cloudy.spout.OrderBaseSpout;

public class Topo {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new TestSpout(), 1);
		builder.setBolt("filter", new TestBolt() , 1).shuffleGrouping("spout") ;
		
		Config conf = new Config() ;
		conf.setDebug(false);
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
