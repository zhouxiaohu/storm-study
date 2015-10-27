/**
  * project：storm-study
  * file：Utils.java
  * author：zxh
  * time：2015年10月15日 上午11:24:30
  * description：
  */
package cn.com.dimensoft.storm.util;

/**
 * class： Utils
 * package： cn.com.dimensoft.storm.cutil
 * author：zxh
 * time： 2015年10月15日 上午11:24:30
 * description： 
 */
public class Utils {
	
	/**
	 * 
	 * name：waitForSeconds
	 * author：zxh
	 * time：2015年10月15日 上午11:24:46
	 * description：睡眠的秒数
	 * @param seconds
	 */
	public static void waitForSeconds(int seconds) {
		try {
			Thread.sleep(seconds * 1000);
		} catch (InterruptedException e) {
		}
	}

	/**
	 * 
	 * name：waitForMillis
	 * author：zxh
	 * time：2015年10月15日 上午11:24:49
	 * description：睡眠的毫秒数
	 * @param milliseconds
	 */
	public static void waitForMillis(long milliseconds) {
		try {
			Thread.sleep(milliseconds);
		} catch (InterruptedException e) {
		}
	}
}
