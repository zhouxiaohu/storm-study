/**
  * project：storm-study
  * file：FunctionForSplit.java
  * author：zxh
  * time：2015年10月16日 下午3:38:48
  * description：
  */
package cn.com.dimensoft.storm.trident.function;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

/**
 * class： FunctionForSplit
 * package： cn.com.dimensoft.storm.trident.function
 * author：zxh
 * time： 2015年10月16日 下午3:38:48
 * description： 对sentence进行分割
 */
public class FunctionForSplit extends BaseFunction {

	/**
	 * long:serialVersionUID  
	 * description：
	 */
	private static final long serialVersionUID = 1L;



	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {

		String words = tuple.getString(0);

		for (String word : words.split("\\s+")) {
			collector.emit(new Values(word));
		}
	}

}
