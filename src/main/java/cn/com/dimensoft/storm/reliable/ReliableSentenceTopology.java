/**
  * project：storm-study
  * file：ReliableTopology.java
  * author：zxh
  * time：2015年10月15日 下午4:23:03
  * description：
  */
package cn.com.dimensoft.storm.reliable;

import cn.com.dimensoft.storm.util.Utils;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * class： ReliableTopology
 * package： cn.com.dimensoft.storm.reliable
 * author：zxh
 * time： 2015年10月15日 下午4:23:03
 * description： 添加了ack和fail机制的可靠topology
 * ack和fail的处理逻辑是在spout中定义，每一层的bolt都必须在处理完成之后调用一下ack方法，
 * 或者针对特殊情况调用fail方法，ack方法不会在用户调用的时候立即触发，
 * 而是在处理该tuple的末端的bolt调用了ack后才会真正调用ack（例如一条tuple会被一系列链式的bolt处理，
 * 那么在中间任何一个bolt调用ack时都不会立即触发ack，只有在最末尾的bolt针对该tuple调用了ack之后，
 * ack才会被真正调用，但是中间的每个bolt也必须要调用ack），而fail则是会立即触发。
 */
public class ReliableSentenceTopology {

	/**
	 * name：main
	 * author：zxh
	 * time：2015年10月15日 下午4:23:03
	 * description：
	 * @param args
	 */
	public static void main(String[] args) {

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new ReliableSentenceSpout());

		builder.setBolt("split", new ReliableSentenceSplitBolt())
				.shuffleGrouping("spout");

		builder.setBolt("count", new ReliableSentenceWordCountBolt())
				.fieldsGrouping("split", new Fields("word"));

		LocalCluster local = new LocalCluster();

		local.submitTopology("reliable", new Config(), builder.createTopology());

		Utils.waitForSeconds(1000);

		local.killTopology("reliable");

		local.shutdown();
	}

}
