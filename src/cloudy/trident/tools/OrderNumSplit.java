package cloudy.trident.tools;

import cloudy.tools.DateFmt;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class OrderNumSplit extends BaseFunction{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	String patten = null;
	public OrderNumSplit(String patten)
	{
		this.patten = patten ;
	}
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
//		System.err.println(tuple);
		//"order_id","order_amt","create_date","province_id","cf"
		if (! tuple.isEmpty()) {
			String msg = tuple.getString(0);
			String value[] = msg.split(this.patten) ;
			System.err.println("msg="+msg);
			collector.emit(new Values(value[0],Double.parseDouble(value[1]),DateFmt.getCountDate(value[2], DateFmt.date_short),"orderNum_"+value[3],"cf"));
//			System.err.println(msg);
		}
	}
	
	

}
