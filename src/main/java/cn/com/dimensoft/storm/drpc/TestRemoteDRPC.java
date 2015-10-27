/**
  * project：storm-study
  * file：TestRemoteDRPC.java
  * author：zxh
  * time：2015年10月27日 上午9:16:17
  * description：
  */
package cn.com.dimensoft.storm.drpc;

import org.apache.thrift7.TException;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;

/**
 * class： TestRemoteDRPC
 * package： cn.com.dimensoft.storm.drpc
 * author：zxh
 * time： 2015年10月27日 上午9:16:17
 * description： 测试RemoteDRPCTopology
 */
public class TestRemoteDRPC {

	/**
	 * name：main
	 * author：zxh
	 * time：2015年10月27日 上午9:16:17
	 * description：
	 * @param args
	 * @throws DRPCExecutionException 
	 * @throws TException 
	 */
	public static void main(String[] args) throws TException,
			DRPCExecutionException {

		// 第一个参数要和storm.yaml中配置的drpc.servers保持一致
		// 第二个参数是drpc端口号，默认是3772
		DRPCClient client = new DRPCClient("hadoop-yarn01.dimensoft.com.cn",
				3772);

		// 第一个参数要和创建DRPCTopology时的function参数名保持一致
		// new LinearDRPCTopologyBuilder("remoterpc");
		// 第二个参数是传递过去需要处理的数据，会传递给bolt进行处理
		System.err.println(client.execute("remoterpc", "today is a nice day"));
	}

}
