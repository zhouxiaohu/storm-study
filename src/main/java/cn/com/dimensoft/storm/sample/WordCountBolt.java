/**
  * project：storm-test
  * file：WordCountBolt.java
  * author：zxh
  * time：2015年9月23日 下午2:29:39
  * description：
  */
package cn.com.dimensoft.storm.sample;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * class： WordCountBolt
 * package： cn.com.dimensoft.storm
 * author：zxh
 * time： 2015年9月23日 下午2:29:39
 * description： 
 */
public class WordCountBolt extends BaseBasicBolt {

	/**
	 * long:serialVersionUID  
	 * description：
	 */
	private static final long serialVersionUID = 7683600247870291231L;

	private static Map<String, Integer> map = new HashMap<String, Integer>();

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		// 根据变量名称获得上一个bolt传递过来的数据
		String word = input.getStringByField("word");

		Integer count = map.get(word);
		if (count == null) {
			map.put(word, 1);
		} else {
			count ++;
			map.put(word, count);
		}

		System.out.println(map);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}
