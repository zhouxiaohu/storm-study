/**
  * project：storm-study
  * file：WordAggregator.java
  * author：zxh
  * time：2015年10月19日 下午5:34:09
  * description：
  */
package cn.com.dimensoft.storm.trident.aggregator;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * class： WordAggregator
 * package： cn.com.dimensoft.storm.trident.aggregator
 * author：zxh
 * time： 2015年10月19日 下午5:34:09
 * description： 这个aggregator是一batch为单位进行处理的，并不会对所有batch的结果进行汇总
 */
public class WordAggregator extends BaseAggregator<Map<String, Integer>> {

	/**
	 * long:serialVersionUID  
	 * description：
	 */    
	private static final long serialVersionUID = 1L;

	
	@Override
	public HashMap<String, Integer> init(Object batchId, TridentCollector collector) {
		return new HashMap<String, Integer>();
	}

	@Override
	public void aggregate(Map<String, Integer> map, TridentTuple tuple,
			TridentCollector collector) {
		
		map.put(tuple.getString(0), map.get(tuple.getString(0)) == null ? 1 :  map.get(tuple.getString(0)) + 1);
	}

	@Override
	public void complete(Map<String, Integer> map, TridentCollector collector) {
		collector.emit(new Values(map));
	}

}
