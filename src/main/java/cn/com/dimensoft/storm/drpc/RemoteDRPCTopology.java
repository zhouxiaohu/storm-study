/**
  * project：storm-study
  * file：RemoteDRPCTopology.java
  * author：zxh
  * time：2015年10月27日 上午8:37:03
  * description：
  */
package cn.com.dimensoft.storm.drpc;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * class： RemoteDRPCTopology
 * package： cn.com.dimensoft.storm.drpc
 * author：zxh
 * time： 2015年10月27日 上午8:37:03
 * description： 使用RemoteDRPC来测试DRPC，将用户输入的字符串参数转化为大写
 */
public class RemoteDRPCTopology {

	/**
	 * name：main
	 * author：zxh
	 * time：2015年10月27日 上午8:37:03
	 * description：
	 * @param args
	 * @throws InvalidTopologyException 
	 * @throws AlreadyAliveException 
	 */
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

		// 创建topology
		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder(
				"remoterpc");
		// 添加bolt到topology，这里已经不需要添加spout了，因为输入数据源是由客户端来输入的
		builder.addBolt(new SampleBolt());
		// 提交到集群运行
		StormSubmitter.submitTopology("drpc", new Config(),
				builder.createRemoteTopology());
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
