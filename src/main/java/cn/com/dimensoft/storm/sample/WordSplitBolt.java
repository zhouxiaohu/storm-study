/**
  * project：storm-test
  * file：WordBolt.java
  * author：zxh
  * time：2015年9月23日 下午2:29:12
  * description：
  */
package cn.com.dimensoft.storm.sample;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * class： WordSplitBolt
 * package： cn.com.dimensoft.storm
 * author：zxh
 * time： 2015年9月23日 下午2:29:12
 * description： 
 */
public class WordSplitBolt extends BaseBasicBolt {

	/**
	 * long:serialVersionUID  
	 * description：
	 */
	private static final long serialVersionUID = -1904854284180350750L;

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// 根据变量名获得从spout传来的值
		String line = input.getStringByField("line");

		System.out.println("============================== " + line + " ==============================");
		
		// 对单词进行分割
		for (String word : line.split(" ")) {
			// 传递给下一个组件，即WordCountBolt
			collector.emit(new Values(word));
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// 声明本次emit出去的变量名称
		declarer.declare(new Fields("word"));
	}

}
