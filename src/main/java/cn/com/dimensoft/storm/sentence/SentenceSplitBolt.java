/**
  * project：storm-study
  * file：SentenceSplitBolt.java
  * author：zxh
  * time：2015年10月15日 上午11:30:18
  * description：
  */
package cn.com.dimensoft.storm.sentence;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * class： SentenceSplitBolt
 * package： cn.com.dimensoft.storm.sentence
 * author：zxh
 * time： 2015年10月15日 上午11:30:18
 * description： 分割SentenceSpout传递过来的sentence
 */
public class SentenceSplitBolt extends BaseBasicBolt {

	/**
	 * long:serialVersionUID  
	 * description：
	 */    
	private static final long serialVersionUID = 1L;

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String sentence = input.getStringByField("sentence");
		
		for(String word : sentence.split(" ")){
			collector.emit(new Values(word));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
