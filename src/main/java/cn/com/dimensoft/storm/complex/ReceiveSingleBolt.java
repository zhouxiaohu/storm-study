package cn.com.dimensoft.storm.complex;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * 
 * class： ReceiveSingleBolt
 * package： cn.com.dimensoft.storm
 * author：zxh
 * time： 2015年10月12日 上午11:03:34
 * description：处理IntegerSpout发送过来的奇数
 */
public class ReceiveSingleBolt extends BaseBasicBolt {

	/**
	 * long:serialVersionUID  
	 * description：
	 */
	private static final long serialVersionUID = -1904854284180350750L;

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// 根据变量名获得从spout传来的值
		int number = input.getIntegerByField("number");

		System.err.println("I'm ReceiveSingleBolt, i receive: " + number);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
