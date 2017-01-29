package cloudy.bolt;

import java.util.Map;

import cloudy.tools.DateFmt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class LogFmtBolt implements IBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String logString = input.getString(0) ;
		try {
			if(input != null)
			{
				String arr[] = logString.split("\\t");
				collector.emit(new Values(DateFmt.getCountDate(arr[2], DateFmt.date_short),arr[1])) ;
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
		
		
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

		declarer.declare(new Fields("date","session_id")) ;
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
