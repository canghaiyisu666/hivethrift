package com.scistor;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TCloseOperationReq;
import org.apache.hive.service.cli.thrift.TCloseSessionReq;
import org.apache.hive.service.cli.thrift.TExecuteStatementReq;
import org.apache.hive.service.cli.thrift.TExecuteStatementResp;
import org.apache.hive.service.cli.thrift.TFetchOrientation;
import org.apache.hive.service.cli.thrift.TFetchResultsReq;
import org.apache.hive.service.cli.thrift.TFetchResultsResp;
import org.apache.hive.service.cli.thrift.TGetLogReq;
import org.apache.hive.service.cli.thrift.TGetLogResp;
import org.apache.hive.service.cli.thrift.TOpenSessionReq;
import org.apache.hive.service.cli.thrift.TOpenSessionResp;
import org.apache.hive.service.cli.thrift.TOperationHandle;
import org.apache.hive.service.cli.thrift.TRow;
import org.apache.hive.service.cli.thrift.TRowSet;
import org.apache.hive.service.cli.thrift.TSessionHandle;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;

public class ThriftSubmit {

	public static void main(String[] args) throws TException, IOException,
			InterruptedException {
		TSocket transport = new TSocket("192.168.8.102", 10000);
		TProtocol proto = new TBinaryProtocol(transport);
		TCLIService.Client client = new TCLIService.Client(proto);

		transport.open();
		System.out.println("transport opened");

		TOpenSessionReq openReq = new TOpenSessionReq();
		System.out.println("    	TOpenSessionReq ");

		TOpenSessionResp openResp = client.OpenSession(openReq);
		System.out.println("    	TOpenSessionResp ");

		TSessionHandle sessHandle = openResp.getSessionHandle();
		System.out.println("    	TSessionHandle ");

		TExecuteStatementReq execReq0 = new TExecuteStatementReq(sessHandle,
				"use tpctest");
		TExecuteStatementReq execReq = new TExecuteStatementReq(sessHandle,
				"select count(*) from web_site ");

		execReq.setRunAsync(true);

		client.ExecuteStatement(execReq0);//

		TExecuteStatementResp execResp = client.ExecuteStatement(execReq);

		System.out.println("statement executed " + execResp.getStatus());

		TOperationHandle stmtHandle = execResp.getOperationHandle();
		System.out.println("OperationHandler " + stmtHandle.getOperationId()
				+ ", toString\n" + stmtHandle.toString());

		TGetLogReq req = new TGetLogReq(stmtHandle);
		TGetLogResp log = client.GetLog(req);

		System.out.println("======111===" + log.getLog() + "=00==");

		// // log.read(proto);
		// // System.out.println("======222==="+log.getLog());
		//
		// // ScistorHiveOp thisOp = new ScistorHiveOp(Thread.currentThread()
		// // .getStackTrace()[1].getClassName(), query);
		TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
		byte[] stmt = serializer.serialize(stmtHandle);
		FileOutputStream out = new FileOutputStream("tmp");
		out.write(stmt);
		out.close();
		// // FileOutputStream fos = new FileOutputStream("hiveOp.out");
		//
		// // ObjectOutputStream oos = new ObjectOutputStream(fos);
		// // oos.writeObject(thisOp);

		Thread.sleep(120000);
		System.out.println("====Thread.sleep(120000)======");
		log.clear();
		log.read(proto);
		System.out.println("======333===" + log.getLog());

		TFetchResultsReq fetchReq = new TFetchResultsReq(stmtHandle,
				TFetchOrientation.FETCH_FIRST, 1);

		TFetchResultsResp resultsResp = client.FetchResults(fetchReq);
		System.out.println("FetchResults " + resultsResp.getStatus());
		TRowSet resultsSet = resultsResp.getResults();

		List<TRow> resultRows = resultsSet.getRows();
		System.out.println(resultsSet.isSetColumns() + ", "
				+ resultsSet.isSetRows());
		System.out.println(resultRows.size() + " got from query"
				+ resultsSet.getColumns() + ", " + resultsSet.getRows());
		for (TRow resultRow : resultRows) {
			resultRow.toString();
			System.out.println("result got" + resultRow.getColVals());
		}

		TCloseOperationReq closeReq = new TCloseOperationReq();
		closeReq.setOperationHandle(stmtHandle);
		client.CloseOperation(closeReq);
		TCloseSessionReq closeConnectionReq = new TCloseSessionReq(sessHandle);
		client.CloseSession(closeConnectionReq);

		transport.close();
	}
}
