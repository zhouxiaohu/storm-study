/**
  * project：storm-study
  * file：SentenceWordCountBolt.java
  * author：zxh
  * time：2015年10月15日 下午1:47:52
  * description：
  */
package cn.com.dimensoft.storm.sentence;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * class： SentenceWordCountBolt
 * package： cn.com.dimensoft.storm.sentence
 * author：zxh
 * time： 2015年10月15日 下午1:47:52
 * description： 统计wordcount
 */
public class SentenceWordCountBolt extends BaseBasicBolt {

	private HashMap<String, Integer> result;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		result = new HashMap<String, Integer>();
	}

	/**
	 * long:serialVersionUID  
	 * description：
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		String word = input.getStringByField("word");

		Integer count = result.get(word);

		if (count == null) {
			count = 0;
		}
		count++;
		result.put(word, count);

		collector.emit(new Values(word, count));

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));
	}

}
