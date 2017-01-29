package cloudy.svlt;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

import backtype.storm.utils.DRPCClient;
import backtype.storm.utils.Utils;
import cloudy.hbase.dao.HBaseDAO;
import cloudy.tools.DateFmt;
import cloudy.trident.tools.Province;
import cloudy.vo.AreaVo;

public class SaleTopSvlt extends HttpServlet {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	String today = null;
	
	public void init() throws ServletException {
		today = DateFmt.getCountDate(null, DateFmt.date_short);
	}

	public void destroy() {
		super.destroy();
	}

	public void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		this.doPost(request, response);
	}

	public void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		request.setCharacterEncoding("utf-8");
		response.setContentType("text/html;charset=utf-8");
//		hisDay = DateFmt.getCountDate(null, DateFmt.date_short, -1);// 取昨天
//		System.out.println("hisDay=" + hisDay);
//		System.out.println("hisData=" + hisData);
//		while (true) {
			
//			if (!dateStr.equals(today)) {
//				// 跨天处理
//				today = dateStr;
//			}
			
			// String data = this.getData(today, dao);
			
			/**
			 * 
			 * [["2014-08-19:cf:amt_1 2014-08-19:cf:amt_2","2014-08-19:cf:amt_1","2014-08-19","cf","amt_1",1842.7999999999995],
			 * ["2014-08-19:cf:amt_1 2014-08-19:cf:amt_2","2014-08-19:cf:amt_2","2014-08-19","cf","amt_2",1773.499999999999]]
			 * 
			 * 
			 */
			Map<String, String> provMap = Province.getProvMap() ;
			// 每个5s查询
			DRPCClient client = new DRPCClient("192.168.1.107", 3772);
			try {
				while (true) {
					String todayColumn = "[";//柱子图value
					String todaySpline = "[";//折线图value

					String xName = "[";
					String dateStr = DateFmt.getCountDate(null, DateFmt.date_short);
					
					String amtArgs = this.getArgs("amt", dateStr, provMap) ;
					String saleStr = client.execute("getOrderAmt",amtArgs) ;
					
					System.err .println("销售额：" + saleStr);
					String amtArr[] = saleStr.split("\\],\\[") ;
					System.out.println(amtArr[4]);
					List<String> top5List = new ArrayList<String>();
					for (String amt : amtArr) {
						System.out.println("amt="+amt);
						String provId = amt.split(",")[4].replaceAll("\"", "").split("_")[1] ;
						top5List.add(provId);
						xName += provMap.get(provId)+",";
						
						todayColumn += getFmtPoint(amt.split(",")[5].replaceAll("\\]\\]", "")) +",";
					}
					xName = xName.substring(0, xName.length() - 1)+"]" ;
					System.out.println(xName);
					
					todayColumn = todayColumn.substring(0, todayColumn.length()-1)+"]" ;
					System.err.println("todayColumn="+todayColumn);
					
					
					String orderNumArgs = this.getArgs("orderNum", dateStr, provMap) ;
					
					String orderStr = client.execute("getOrderNum",orderNumArgs) ;
					
					//订单数，需要取出销售额前5名的
					System.err .println("订单数：" + orderStr);
					String orderArr[] = orderStr.split("\\],\\[") ;
//					int a=0;
					Map<String, String> orderMap = new HashMap<String, String>();
					for (String order : orderArr) {
						String provId = order.split(",")[4].replaceAll("\"", "").split("_")[1] ;
						orderMap.put(provId, order.split(",")[5].replaceAll("\\]\\]", ""));
//						a++;
//						todaySpline += order.split(",")[5].replaceAll("\\]\\]", "") +",";
//						if (a==5) {
//							break;
//						}
					}
					for (String provId:top5List) {
						todaySpline += orderMap.get(provId) +",";
					}
					todaySpline = todaySpline.substring(0, todaySpline.length()-1)+"]" ;
					System.err.println("todaySpline="+todaySpline);
					
					Utils.sleep(5000);
					
					String jsDataString = "{\'todayColumn\':" + todayColumn + ",\'todaySpline\':" + todaySpline 
						+ ",\'xName\':\'" + xName 
						+"\'}";

					boolean flag = this.sentData("jsFun", response, jsDataString);
					if (!flag) {
						break;
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

			
//		}
	}

	public boolean sentData(String jsFun, HttpServletResponse response,
			String data) {
		try {
			response.setContentType("text/html;charset=utf-8");
			response.getWriter().write(
					"<script type=\"text/javascript\">parent." + jsFun + "(\""
							+ data + "\")</script>");
			response.flushBuffer();
			return true;
		} catch (Exception e) {
			System.out.println(" long connect 已断开 ");
			return false;
		}
	}

	public String getArgs(String flag, String dateStr, Map<String, String> provMap)
	{
		String args = "" ;
		//2014-08-20:cf:amt_1
		for (String str : provMap.keySet()) {
			args += dateStr+":cf:"+flag+"_"+str+" " ;
		}
		if (args != null) {
			args = args.trim() ;
		}
		return args;
	}
	
//	public String getData(String date, HBaseDAO dao) {
//		List<Result> list = dao.getRows("area_order", date);
//		AreaVo vo = new AreaVo();
//		for (Result rs : list) {
//			String rowKey = new String(rs.getRow());
//			String aredid = null;
//			if (rowKey.split("_").length == 2) {
//				aredid = rowKey.split("_")[1];
//			}
//			for (KeyValue keyValue : rs.raw()) {
//				if ("order_amt".equals(new String(keyValue.getQualifier()))) {
//					vo.setData(aredid, new String(keyValue.getValue()));
//					break;
//				}
//			}
//		}
//		String result = "[" + getFmtPoint(vo.getBeijing()) + ","
//				+ getFmtPoint(vo.getShanghai()) + ","
//				+ getFmtPoint(vo.getGuangzhou()) + ","
//				+ getFmtPoint(vo.getShenzhen()) + ","
//				+ getFmtPoint(vo.getChengdu()) + "]";
//		return result;
//
//	}

	public String getFmtPoint(String str) {
		DecimalFormat format = new DecimalFormat("#");
		if (str != null) {
			return format.format(Double.parseDouble(str));
		}
		return null;
	}

}
