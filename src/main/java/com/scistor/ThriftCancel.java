package com.scistor;

import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TCancelOperationReq;
import org.apache.hive.service.cli.thrift.TCancelOperationResp;
import org.apache.hive.service.cli.thrift.TCloseOperationReq;
import org.apache.hive.service.cli.thrift.TCloseSessionReq;
import org.apache.hive.service.cli.thrift.TOpenSessionReq;
import org.apache.hive.service.cli.thrift.TOpenSessionResp;
import org.apache.hive.service.cli.thrift.TOperationHandle;
import org.apache.hive.service.cli.thrift.TSessionHandle;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;

public class ThriftCancel {
	public static void main(String[] args) throws TException, IOException {
		TSocket transport = new TSocket("192.168.8.102", 10000);

		TCLIService.Client client = new TCLIService.Client(new TBinaryProtocol(
				transport));

		transport.open();
		System.out.println("transport opened");

		TOpenSessionReq openReq = new TOpenSessionReq();
		System.out.println("    	TOpenSessionReq ");

		TOpenSessionResp openResp = client.OpenSession(openReq);
		System.out.println("    	TOpenSessionResp ");

		TSessionHandle sessHandle = openResp.getSessionHandle();
		System.out.println("    	TSessionHandle ");

		TCancelOperationReq cancelReq = new TCancelOperationReq();
		TOperationHandle stmtHandle = new TOperationHandle();

		FileInputStream in = new FileInputStream("tmp");
		byte[] stmt = new byte[100];
		System.out.println(in.read(stmt)+"======READREADREAD====");

		TDeserializer de = new TDeserializer();
		System.out.println("stmtHandle " + stmtHandle);
		de.deserialize(stmtHandle, stmt);
		System.out.println("stmtHandle " + stmtHandle);

		cancelReq.setOperationHandle(stmtHandle);
		TCancelOperationResp cancelResp = client.CancelOperation(cancelReq);
		System.out.println(cancelResp.getStatus());

		TCloseOperationReq closeReq = new TCloseOperationReq();
		closeReq.setOperationHandle(stmtHandle);
		client.CloseOperation(closeReq);
		TCloseSessionReq closeConnectionReq = new TCloseSessionReq(sessHandle);
		client.CloseSession(closeConnectionReq);
		
		
		in.close();
		transport.close();
	}
}
