package io.github.kimmking.activemq.test;

import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;

public class TestReceiver {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		int a = 0;
		if(a== 0)
			listen();
		else
			receive();

	}

	private static void receive() {
		try {
			// init connection factory with activemq
			QueueConnectionFactory factory = new ActiveMQConnectionFactory("tcp://127.0.0.1:61616");
			// specify the destination
			Queue queue = new ActiveMQQueue("kk.mongo");
			// create connection,session,consumer and receive message
			QueueConnection conn = factory.createQueueConnection();
			conn.start();
			QueueSession session = conn.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
			QueueReceiver receiver = session.createReceiver(queue);//, "score=10");

			int index = 0;
			int count = 10000;
			final long[] times = new long[3];
			times[2] = times[0] = System.currentTimeMillis();
			while (index++ < count) {
				Message msg = receiver.receive(800);
				if(msg == null) break;
				//System.out.println("*********");
				//System.out.println(msg.getIntProperty("score"));
				//System.out.println(msg.getText());
//				if((index+1) % 100 == 0) 
//					System.out.println((index+1)+ " - " + msg.getJMSMessageID());
				
				int a = index;
				if( a % 100 == 0)
				{
					times[1] = times[0];
					times[0] = System.currentTimeMillis();
					//System.out.println(times[0] - times[1]);
					
					System.out.println((times[0] - times[1]) + " -> " +((a+1)*1000.0)/(times[0] - times[2]));
					
				}
			}
			
			long end = System.currentTimeMillis();
			System.out.println("receive " + (index-1) + " messages in " + (end-times[2])/1000.0 + " s");

			session.close();
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	private static void listen() {
		try {
			// init connection factory with activemq
			QueueConnectionFactory factory = new ActiveMQConnectionFactory("tcp://127.0.0.1:61616");
			// specify the destination
			Queue queue = new ActiveMQQueue("kk.mongo");
			// create connection,session,consumer and receive message
			ActiveMQConnection conn = (ActiveMQConnection) factory.createQueueConnection();
			//conn.setOptimizeAcknowledge(true);
			//conn.setOptimizeAcknowledgeTimeOut(4000);
			//conn.setOptimizedAckScheduledAckInterval(2000);
			//conn.setSendAcksAsync(true);
			conn.start();
			final int count = 10000;
			// first receiver on broker1
			QueueSession sessionA1 = conn.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
			QueueReceiver receiverA1 = sessionA1.createReceiver(queue);
			final AtomicInteger a1 = new AtomicInteger(0);
			final long[] times = new long[3];
			times[2] = times[0] = System.currentTimeMillis();
			MessageListener listenerA1 = new MessageListener(){
				public void onMessage(Message message) {
					int a = a1.getAndIncrement();
					if( a % 2 == 0)
					{
						times[1] = times[0];
						times[0] = System.currentTimeMillis();
						//System.out.println(times[0] - times[1]);
						
						System.out.println((times[0] - times[1]) + " -> " +((a+1)*1000.0)/(times[0] - times[2]));
						
					}
					if(a == count - 1){
						System.out.println("onMessage " + count + " message for " + (System.currentTimeMillis()-times[2])/1000.0 + " s");
					}
					
					
//					try {
//						System.out.println(aint1.incrementAndGet()+" => A1 receive from kk.mongo: " + ((TextMessage)message).getText());
//					} catch (JMSException e) {
//						e.printStackTrace();
//					}
				}};
			receiverA1.setMessageListener(listenerA1 );
			sessionA1.run();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

}
