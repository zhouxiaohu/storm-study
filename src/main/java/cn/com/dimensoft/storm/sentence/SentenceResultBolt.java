/**
  * project：storm-study
  * file：SentenceResultBolt.java
  * author：zxh
  * time：2015年10月15日 下午2:07:22
  * description：
  */
package cn.com.dimensoft.storm.sentence;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * class： SentenceResultBolt
 * package： cn.com.dimensoft.storm.sentence
 * author：zxh
 * time： 2015年10月15日 下午2:07:22
 * description： 上报结果
 */
public class SentenceResultBolt extends BaseBasicBolt {

	/**
	 * long:serialVersionUID  
	 * description：
	 */
	private static final long serialVersionUID = 1L;

	private HashMap<String, Integer> result;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		result = new HashMap<String, Integer>();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		
		String word = input.getStringByField("word");
		Integer count = input.getIntegerByField("count");
		
		result.put(word, count);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void cleanup() {
		
		System.err.println("==================FINAL RESULT==================");
		
		for(Entry<String, Integer> entry : result.entrySet()){
			System.err.println(entry.getKey() + " = " + entry.getValue());
		}
	}

}
