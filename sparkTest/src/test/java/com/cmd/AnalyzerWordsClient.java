package com.cmd;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * blog http://www.micmiu.com
 *
 * @author Michael
 *
 */
public class AnalyzerWordsClient {

	public static final String SERVER_IP = "101.201.116.215";
	public static final int SERVER_PORT = 8888;
	public static final int TIMEOUT = 10000;

	/**
	 *
	 * @param words
	 */
	public void startClient(String words) {
//		TTransport transport = null;
//		try {
//			transport = new TSocket(SERVER_IP, SERVER_PORT, TIMEOUT);
//			// 协议要和服务端一致
//			TProtocol protocol = new TBinaryProtocol(transport);
//			SparkDriverService.Client client = new SparkDriverService.Client(
//					protocol);
//			transport.open();
//			
//			String str = client.commitJobToSpark("--master spark://spark1:7077 --total-executor-cores 32 --executor-memory 20g /home/spark/test/sqlTest.jar >> log");
//		} catch (TTransportException e) {
//			e.printStackTrace();
//		} catch (TException e) {
//			e.printStackTrace();
//		} finally {
//			if (null != transport) {
//				transport.close();
//			}
//		}
		
		TTransport transport = null;
		try {
			TSocket tSocket = new TSocket("101.201.116.215",8888,3000);
			transport = new TFramedTransport(tSocket);
			TProtocol protocol = new TCompactProtocol(transport);
			TMultiplexedProtocol mp= new TMultiplexedProtocol(protocol,"SparkDriverService");
			SparkDriverService.Client client = new SparkDriverService.Client(mp);
			transport.open();
			String msg = client.commitJobToSpark(" --master spark://spark1:7077 --total-executor-cores 32 --executor-memory 20g   --class com.spark.sql.executor.ProductSaleReportExecutor  /data/server/lxy_spark/wmsSparkWorker.jar >>/data/server/lxy_spark/log");
			System.out.println("Thrift client result =: " + msg);
			tSocket.close();
		} catch (TTransportException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		} finally {
			if (null != transport) {
				transport.close();
			}
		}

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
//		String temp = Product.read("e:/data","utf-8");
		AnalyzerWordsClient client = new AnalyzerWordsClient();
		client.startClient("sadfsdfsd四的发生的发生是否沙发按说");
	}

}