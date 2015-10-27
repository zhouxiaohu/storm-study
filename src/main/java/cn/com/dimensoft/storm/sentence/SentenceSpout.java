/**
  * project：storm-study
  * file：SentenceSpout.java
  * author：zxh
  * time：2015年10月15日 上午10:39:52
  * description：
  */
package cn.com.dimensoft.storm.sentence;

import java.util.Map;

import cn.com.dimensoft.storm.util.Utils;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * class： SentenceSpout
 * package： cn.com.dimensoft.storm.sentence
 * author：zxh
 * time： 2015年10月15日 上午10:39:52
 * description： SentenceSpout产生数据源
 */
public class SentenceSpout extends BaseRichSpout {

	/**
	 * long:serialVersionUID  
	 * description：
	 */
	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;

	private int index;

	private String[] sentences = { //
			"my dog has fleas", //
			"i like cold beverages", //
			"the dog ate my homeword", //
			"don't have a cow man", //
			"i don't think i like fleas" //
	};

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {

		this.collector = collector;
	}

	@Override
	public void nextTuple() {

		collector.emit(new Values(sentences[index++]));
		
		if (index >= sentences.length) {
			index = 0;
		}

		Utils.waitForMillis(1);
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

}
