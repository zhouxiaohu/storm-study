package cn.com.dimensoft.storm.complex;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * 
 * class： ReceiveAllBolt
 * package： cn.com.dimensoft.storm
 * author：zxh
 * time： 2015年10月12日 上午11:03:38
 * description：处理IntegerSpout发送过来的偶数和StringSpout发送过来的所有数据
 */
public class ReceiveAllBolt extends BaseBasicBolt {

	/**
	 * long:serialVersionUID  
	 * description：
	 */
	private static final long serialVersionUID = -1904854284180350750L;

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// 根据变量名获得从spout传来的值
		Object obj = input.getValue(0);

		System.err.println("I'm ReceiveAllBolt, i receive: " + obj);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}
}
