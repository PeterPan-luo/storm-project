package cloudy.trident.drpc;

import backtype.storm.LocalDRPC;
import backtype.storm.utils.DRPCClient;
import backtype.storm.utils.Utils;

public class TridentDRPCclient {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		DRPCClient client = new DRPCClient("192.168.112.112", 3772);
//		LocalDRPC client = new LocalDRPC();
		try {
				while (true) 
				{
					System.err.println("销售额："+client.execute("getOrderAmt", "2014-09-15:cf:amt_1 2014-09-15:cf:amt_2 2014-09-15:cf:amt_3 2014-09-15:cf:amt_4 2014-09-15:cf:amt_5 2014-09-15:cf:amt_6 2014-09-15:cf:amt_7 2014-09-15:cf:amt_8")) ;
					System.err.println("订单数："+client.execute("getOrderNum", "2014-09-15:cf:orderNum_1 2014-09-15:cf:orderNum_2 2014-09-15:cf:orderNum_3 2014-09-15:cf:orderNum_4 2014-09-15:cf:orderNum_5 2014-09-15:cf:orderNum_6 2014-09-15:cf:orderNum_7 2014-09-15:cf:orderNum_8")) ;
					Utils.sleep(5000);
				}
			} catch (Exception e) {
				e.printStackTrace() ;
			}
		
	}
}
