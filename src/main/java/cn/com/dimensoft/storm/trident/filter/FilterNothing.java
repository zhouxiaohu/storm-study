/**
  * project：storm-study
  * file：FilterNothing.java
  * author：zxh
  * time：2015年10月12日 下午9:30:46
  * description：
  */
package cn.com.dimensoft.storm.trident.filter;

import java.util.Map;

import storm.trident.operation.Filter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

/**
 * class： FilterNothing
 * package： cn.com.dimensoft.storm.trident.filter
 * author：zxh
 * time： 2015年10月12日 下午9:30:46
 * description： 
 */
public class FilterNothing implements Filter {

	/**
	 * long:serialVersionUID  
	 * description：
	 */
	private static final long serialVersionUID = 1L;

	private int partitionIndex;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		this.partitionIndex = context.getPartitionIndex();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see storm.trident.operation.Operation#cleanup()
	 */
	@Override
	public void cleanup() {
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * storm.trident.operation.Filter#isKeep(storm.trident.tuple.TridentTuple)
	 */
	@Override
	public boolean isKeep(TridentTuple tuple) {

		System.err.println("I am nothing filter partition [ " + partitionIndex
				+ " ]" + tuple);
		// System.err.println(tuple);
		return true;
	}

}
