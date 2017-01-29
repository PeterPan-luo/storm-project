package cloudy.bolt;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import cloudy.tools.DateFmt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class UVBolt1 implements IRichBolt {

	/**
	 * 
	 */
	Map<String, Long> map = new HashMap<String, Long>() ;
	Set<String> hasEmittedSet = new HashSet<String>() ;
	private static final long serialVersionUID = 1L;

	OutputCollector collector = null;
	String today = null;
	
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void execute(Tuple input) {
		try {
			if (input != null) {
				String date = input.getString(0) ;
				if (today.compareTo(date) < 0 ) {
					//跨天处理
					today = date ;
					map.clear() ;
					hasEmittedSet.clear() ;
				}
				
				String session_id = input.getString(1) ;
				String key = date+"_"+session_id ;
				if (hasEmittedSet.contains(key)) {
					throw new Exception("this tuple has emitted ...") ;
				}
				
				Long pv = map.get(key) ;
				if (pv == null) {
					pv = 0L ;
				}
				pv ++ ;
				map.put(key, pv) ;
				if (pv >= 2) {
					collector.emit(new Values(key)) ;
					hasEmittedSet.add(key) ;
				}
				collector.ack(input);
			}
			
		} catch (Exception e) {
			// TODO: handle exception
//			System.err.println("");
		}
		
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector ;
		today = DateFmt.getCountDate(null, DateFmt.date_short) ;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

		declarer.declare(new Fields("date_SessionId"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
