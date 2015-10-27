/**
  * project：storm-study
  * file：FilterSpecialWord.java
  * author：zxh
  * time：2015年10月16日 下午3:55:09
  * description：
  */
package cn.com.dimensoft.storm.trident.filter;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 * class： FilterSpecialWord
 * package： cn.com.dimensoft.storm.trident.filter
 * author：zxh
 * time： 2015年10月16日 下午3:55:09
 * description： 过滤掉包含特殊单词的tuple
 */
public class FilterSpecialWord extends BaseFilter {

	/**
	 * long:serialVersionUID  
	 * description：
	 */
	private static final long serialVersionUID = 1L;

	private String word;


	public FilterSpecialWord(String word) {
		this.word = word;
	}

	@Override
	public boolean isKeep(TridentTuple tuple) {
		return !tuple.getString(0).contains(word);
	}

}
