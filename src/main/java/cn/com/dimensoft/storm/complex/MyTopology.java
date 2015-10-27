package cn.com.dimensoft.storm.complex;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

/**
 * 
 * class： MyTopology
 * package： cn.com.dimensoft.storm
 * author：zxh
 * time： 2015年10月12日 上午11:03:08
 * description：定义完整的topology，包括spout、bolt以及数据流的grouping
 */
public class MyTopology {

	/**
	 * 
	 * name：main
	 * author：zxh
	 * time：2015年10月12日 上午11:04:07
	 * description：
	 * @param args
	 * @throws AlreadyAliveException
	 * @throws InvalidTopologyException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException, InterruptedException {

		TopologyBuilder builder = new TopologyBuilder();

		Config config = new Config();

		// 配置整个topology的worker数量
		config.setNumWorkers(1);

		// 设置spout
		builder.setSpout("IntegerSpout", //
				new IntegerSpout());

		// 设置spout
		builder.setSpout("StringSpout", //
				new StringSpout());

		// 设置bolt
		// ReceiveSingleBolt这个组件用来处理IntegerSpout中stream的id为single的数据流
		builder.setBolt("ReceiveSingleBolt", //
				new ReceiveSingleBolt()).//
				shuffleGrouping("IntegerSpout", "single");

		// 设置bolt
		// ReceiveAllBolt这个组件用来处理IntegerSpout中stream的id为double的数据流，以及StringSpout这个组件的所有数据流
		builder.setBolt("ReceiveAllBolt", //
				new ReceiveAllBolt()).//
				shuffleGrouping("IntegerSpout", "double").//
				globalGrouping("StringSpout");

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
