/**
  * project：storm-study
  * file：MyTridentTopology.java
  * author：zxh
  * time：2015年10月12日 下午4:41:16
  * description：
  */
package cn.com.dimensoft.storm.trident;

import org.apache.thrift7.TException;

import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.testing.FixedBatchSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import cn.com.dimensoft.storm.trident.filter.FilterNothing;
import cn.com.dimensoft.storm.trident.filter.FilterSpecialWord;
import cn.com.dimensoft.storm.trident.function.FunctionForSplit;
import cn.com.dimensoft.storm.util.Utils;

/**
 * class： MyTridentTopology
 * package： cn.com.dimensoft.storm.trident
 * author：zxh
 * time： 2015年10月12日 下午4:41:16
 * description： 
 */
public class TridentTest {

	/**
	 * 
	 * name：main
	 * author：zxh
	 * time：2015年10月12日 下午4:41:43
	 * description：
	 * @param args
	 * @throws DRPCExecutionException 
	 * @throws TException 
	 */
	public static void main(String[] args) throws TException,
			DRPCExecutionException {

		// 创建配置参数的config
		Config conf = new Config();

		// 本地运行测试topology
		LocalCluster local = new LocalCluster();

		local.submitTopology("test", conf, buildTopology());

		Utils.waitForSeconds(10);
		local.shutdown();
	}

	@SuppressWarnings("unchecked")
	public static StormTopology buildTopology() {

		// 创建一个产生数据源的spout
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), //
				10, //
				new Values("i have a very nice dog"), //
				new Values("it's name is lucy"), //
				new Values("i played with it everyday"), //
				new Values("i want it can grow up quickly"));

		// 一直循环不断的发送这些数据
		// spout.setCycle(true);

		// 创建trident topology
		TridentTopology topology = new TridentTopology();

		// 进行逻辑处理
		// 这个test是指在zookeeper中保存了处理的状态的znode
		topology.newStream("test", spout)
				// 过滤掉包含dog的sentence
				.each(new Fields("sentence"), new FilterSpecialWord("dog"))
				// 对sentence进行分割，分割出来的每个单词再emit出来，变量名为word
				.each(new Fields("sentence"), new FunctionForSplit(),
						new Fields("word"))
				// 按照单词内容进行分组
				.groupBy(new Fields("word"))
				// 统计词频，这里一定要注意，这个统计只是对spout一次批量传递过来的数据进行统计，如果数据是2次批量传递过来的是不会进行汇总统计的
				.aggregate(new Fields("word"), new Count(), new Fields("count"))
				// 打印结果
				.each(new Fields("word", "count"), new FilterNothing())
				.parallelismHint(3);

		return topology.build();
	}
}
