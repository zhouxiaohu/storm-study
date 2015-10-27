/**
  * project：storm-study
  * file：LocalDRPCTopology.java
  * author：zxh
  * time：2015年10月27日 上午8:37:03
  * description：
  */
package cn.com.dimensoft.storm.drpc;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * class： LocalDRPCTopology
 * package： cn.com.dimensoft.storm.drpc
 * author：zxh
 * time： 2015年10月27日 上午8:37:03
 * description： 使用localDRPC来测试DRPC，将用户输入的字符串参数转化为大写
 */
public class LocalDRPCTopology {

	/**
	 * name：main
	 * author：zxh
	 * time：2015年10月27日 上午8:37:03
	 * description：
	 * @param args
	 */
	@SuppressWarnings("deprecation")
	public static void main(String[] args) {

		// 创建topology
		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder(
				"localrpc");
		// 添加bolt到topology，这里已经不需要添加spout了，因为输入数据源是由客户端来输入的
		builder.addBolt(new SampleBolt());
		// 本地运行
		LocalCluster cluster = new LocalCluster();
		// 创建localDRPC
		LocalDRPC drpc = new LocalDRPC();
		cluster.submitTopology("drpc", new Config(),
				builder.createLocalTopology(drpc));
		// 测试
		System.err.println(drpc.execute("localrpc", "hello world"));

		cluster.killTopology("drpc");
		cluster.shutdown();

	}

	public static class SampleBolt extends BaseBasicBolt {

		/**
		 * long:serialVersionUID  
		 * description：
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			String original = input.getString(1);
			// 注意这里的input.getValue(0)是request-id(注意这里不能用getString，因为该值为Long类型)，在emit的时候这个参数要同时emit出去
			collector.emit(new Values(input.getValue(0), original
					.toUpperCase()));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("id", "result"));
		}

	}

}
