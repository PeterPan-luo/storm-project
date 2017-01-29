package test;

import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TestSpout implements IRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	SpoutOutputCollector collector = null;
	Queue<String> queue = new ConcurrentLinkedQueue<String>() ;
	
	
	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		System.err.println("ack-------------"+msgId.toString());
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		System.out.println("fail========="+msgId.toString());
		queue.add(msgId.toString());
	}
	
	@Override
	public void nextTuple() {
		
		String uuid = UUID.randomUUID().toString() ;
//		System.out.println("spout nextTuple "+uuid);
		if (queue.size()>0) {
			collector.emit(new Values(queue.poll()),uuid);
		}
		
//		collector.emit(new Values("testValue"));
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector ;
		for (int i = 0; i < 100; i++) {
			queue.add("aaa"+i);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

		declarer.declare(new Fields("testValue"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
