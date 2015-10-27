/**
  * project：storm-study
  * file：SentenceTopology.java
  * author：zxh
  * time：2015年10月15日 下午2:16:13
  * description：
  */
package cn.com.dimensoft.storm.sentence;

import cn.com.dimensoft.storm.util.Utils;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * class： SentenceTopology
 * package： cn.com.dimensoft.storm.sentence
 * author：zxh
 * time： 2015年10月15日 下午2:16:13
 * description： 
 */
public class SentenceTopology {

	public static void main(String[] args) {

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("sentence-spout", //
				new SentenceSpout(), 2).setNumTasks(6);

		builder.setBolt("split-bolt", //
				new SentenceSplitBolt(), 3).shuffleGrouping("sentence-spout")
				.setNumTasks(6);

		builder.setBolt("count-bolt", //
				new SentenceWordCountBolt(), 3).fieldsGrouping("split-bolt",
				new Fields("word"));

		builder.setBolt("result-bolt", new SentenceResultBolt())
				.globalGrouping("count-bolt");

		LocalCluster local = new LocalCluster();
		Config config = new Config();

		config.setDebug(false);
		config.setNumWorkers(3);

		local.submitTopology("test", config, builder.createTopology());

		Utils.waitForSeconds(10);
		local.killTopology("test");
		local.shutdown();
	}
}
