/**
  * project：storm-study
  * file：ReliableBolt.java
  * author：zxh
  * time：2015年10月15日 下午4:22:44
  * description：
  */
package cn.com.dimensoft.storm.reliable;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * class： ReliableBolt
 * package： cn.com.dimensoft.storm.reliable
 * author：zxh
 * time： 2015年10月15日 下午4:22:44
 * description： 
 */
public class ReliableSentenceSplitBolt extends BaseRichBolt {

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

		String sentence = input.getStringByField("sentence");

		System.err.println(sentence);
		if (sentence.contains("dog")) {
			// 发送fail
			collector.fail(input);
		} else {

			for (String word : sentence.split(" ")) {
				collector.emit(input, new Values(word));
			}
			// 发送ack，这里其实不会立即就调用ReliableSentenceSpout中的ack方法，
			// 因为该tuple还有下游处理的bolt，storm会一直track这个tuple，直到最后一个
			// bolt发送了ack后ReliableSentenceSpout的ack才会被调用，
			// 而fail则不会，在整个topology的任何一个bolt处理中调用了fail的话
			// ReliableSentenceSpout的fail就会立即被调用
			collector.ack(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
