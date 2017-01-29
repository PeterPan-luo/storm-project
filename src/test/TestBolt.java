package test;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class TestBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	OutputCollector collector = null;
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}
int pv=0;
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub

		System.out.println("bolt=========="+input.getString(0)+"---"+pv++);
		collector.fail(input) ;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub

		this.collector = collector ;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
