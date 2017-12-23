package io.github.kimmking.activemq.test;

import javax.jms.BytesMessage;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;

public class TestSender {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			// init connection factory with activemq
			QueueConnectionFactory factory = new ActiveMQConnectionFactory("tcp://127.0.0.1:61616?wireFormat.maxInactivityDuration=300000");
			// specify the destination
			Queue queue = new ActiveMQQueue("kk.mongo");
			// create connection,session,producer and deliver message
			ActiveMQConnection conn = (ActiveMQConnection) factory.createQueueConnection();
			conn.setOptimizeAcknowledge(true);
			conn.setUseAsyncSend(true);
			//conn.setSendAcksAsync(true);
			
			QueueSession session = conn.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
			QueueSender sender = session.createSender(queue);
			long start = System.currentTimeMillis();
			int count = 10;
			int size = 1024;
			for (int i = 0; i < count; i++) {
				//String msgText = "testMessage-" + i;
				BytesMessage msg = session.createBytesMessage();
				msg.writeBytes(createBytesMessage(size));
				//if(i%2 == 1)msg.setIntProperty("score", 10);
				sender.send(msg);
			}
			long end = System.currentTimeMillis();
			System.out.println("send " + count + " messages with " + size + " bytes in " + (end-start)/1000.0 + " s");
			
			
			session.close();
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}

	}
	
	private static byte[] createBytesMessage(int i) {
		byte[] bs = new byte[i];
		for (int j = 0; j < i; j++) {
			bs[j] = 'A';
		}
		return bs;
	}

}
