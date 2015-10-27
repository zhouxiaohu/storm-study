/**
  * project：storm-study
  * file：ReliableSentenceSpout.java
  * author：zxh
  * time：2015年10月15日 下午4:22:25
  * description：
  */
package cn.com.dimensoft.storm.reliable;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import cn.com.dimensoft.storm.util.Utils;

/**
 * class： ReliableSentenceSpout
 * package： cn.com.dimensoft.storm.reliable
 * author：zxh
 * time： 2015年10月15日 下午4:22:25
 * description： 
 */
public class ReliableSentenceSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private ConcurrentHashMap<UUID, Values> pending;

	private int index;

	private String[] sentences = { //
			"my dog has fleas", //
			"i like cold beverages", //
	};

	/**
	 * long:serialVersionUID  
	 * description：
	 */
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {

		this.collector = collector;
		pending = new ConcurrentHashMap<UUID, Values>();
	}

	@Override
	public void nextTuple() {

		if (index < sentences.length) {
			Values values = new Values(sentences[index++]);
			// 产生随机uuid作为tuple唯一标识
			UUID msgId = UUID.randomUUID();
			pending.put(msgId, values);
			collector.emit(values, msgId);
		}

		Utils.waitForMillis(100);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("sentence"));
	}

	@Override
	public void ack(Object msgId) {
		
		// 当接受到bolt发送的ack确认后，将id为msgId的tuple从map中移除
		System.err.println("收到ack确认：" + pending.get(msgId));
		pending.remove(msgId);
	}

	@Override
	public void fail(Object msgId) {

		// 当接受到bolt发送的fail确认后，重新发送改tuple
		System.err.println("收到fail确认：" + pending.get(msgId));
		collector.emit(pending.get(msgId), msgId);
	}

}
