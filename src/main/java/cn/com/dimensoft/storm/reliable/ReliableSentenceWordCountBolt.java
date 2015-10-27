/**
  * project：storm-study
  * file：ReliableSentenceWordCountBolt.java
  * author：zxh
  * time：2015年10月15日 下午1:47:52
  * description：
  */
package cn.com.dimensoft.storm.reliable;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * class： ReliableSentenceWordCountBolt
 * package： cn.com.dimensoft.storm.sentence
 * author：zxh
 * time： 2015年10月15日 下午1:47:52
 * description： 统计wordcount
 */
public class ReliableSentenceWordCountBolt extends BaseRichBolt {

	private OutputCollector collector;
	
	/**
	 * long:serialVersionUID  
	 * description：
	 */    
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String word = input.getStringByField("word");
		System.err.println(word);
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}


}
