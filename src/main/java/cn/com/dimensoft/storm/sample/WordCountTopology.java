/**
  * project：storm-test
  * file：WordCount.java
  * author：zxh
  * time：2015年9月23日 上午11:00:07
  * description：
  */
package cn.com.dimensoft.storm.sample;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * class： WordCountTopology
 * package： cn.com.dimensoft.storm
 * author：zxh
 * time： 2015年9月23日 上午11:00:07
 * description： 
 */
public class WordCountTopology {

	/**
	 * name：main
	 * author：zxh
	 * time：2015年9月23日 上午11:00:08
	 * description：
	 * @param args
	 * @throws InvalidTopologyException 
	 * @throws AlreadyAliveException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException, InterruptedException {

		TopologyBuilder builder = new TopologyBuilder();

		// 设置spout
		builder.setSpout("WordCountSpout", //
				new WordCountSpout());

		// 设置bolt
		builder.setBolt("WordSplitBolt", //
				new WordSplitBolt()).//
				shuffleGrouping("WordCountSpout");

		// 设置bolt
		builder.setBolt("WordCountBolt", //
				new WordCountBolt()).//
				fieldsGrouping("WordSplitBolt", new Fields("word"));

		
		// 本地运行或者提交到集群
		if (args != null && args.length == 1) {

			// 集群运行
			StormSubmitter.submitTopology(args[0], //
					new Config(), //
					builder.createTopology());

		} else {

			// 本地运行
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("local", //
					new Config(),//
					builder.createTopology());
			
			Thread.sleep(10000);
			cluster.shutdown();
		}
	}

}
