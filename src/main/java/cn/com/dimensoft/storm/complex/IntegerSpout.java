package cn.com.dimensoft.storm.complex;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * 
 * class： IntegerSpout
 * package： cn.com.dimensoft.storm
 * author：zxh
 * time： 2015年10月12日 上午11:03:23
 * description：产生0-10的整数
 * 	奇数emit给id为single的stream
 * 	偶数emit给id为double的stream
 */
public class IntegerSpout extends BaseRichSpout {

	/**
	 * long:serialVersionUID
	 * description：
	 */
	private static final long serialVersionUID = 1716894015367004238L;

	private SpoutOutputCollector collector;

	private int index = 0;

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map map, TopologyContext context,
			SpoutOutputCollector collector) {

		this.collector = collector;
	}

	@Override
	public void nextTuple() {

		if (index < 10) {
			if (index % 2 == 1) {
//				奇数发送给id为single的stream
				collector.emit("single", new Values(index));
			} else {
//				偶数发送给id为double的stream
				collector.emit("double", new Values(index));
			}

			index++;
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// 声明传递数据的变量名以及所在stream的id
		declarer.declareStream("single", new Fields("number"));
		declarer.declareStream("double", new Fields("number"));
	}

}
