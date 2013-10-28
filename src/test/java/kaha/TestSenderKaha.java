package kaha;

import java.net.URI;

import javax.jms.BytesMessage;
import javax.jms.Queue;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.xbean.XBeanBrokerFactory;

public class TestSenderKaha {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
//			XBeanBrokerFactory factory = new XBeanBrokerFactory();
//			BrokerService broker = factory.createBroker(new URI("activemq-mysql.xml"));
//			broker.setUseJmx(true);
//			broker.setStartAsync(false);
//			broker.start();
			
			QueueConnectionFactory connfactory = new ActiveMQConnectionFactory("tcp://127.0.0.1:61616?wireFormat.maxInactivityDuration=3000000");
			Queue queue = new ActiveMQQueue("kk.kaha");
			ActiveMQConnection conn = (ActiveMQConnection) connfactory.createQueueConnection();
			conn.setOptimizeAcknowledge(true);
			conn.setUseAsyncSend(true);
			QueueSession session = conn.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
			QueueSender sender = session.createSender(queue);
			long start = System.currentTimeMillis();
			int count = 1000000;
			int size = 128;
			for (int i = 0; i < count; i++) {
				//String msgText = "testMessage-" + i;
				BytesMessage msg = session.createBytesMessage();
				//msg.writeBytes(createBytesMessage(size));
				msg.writeBytes(bytes);
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
	
	static byte[] bytes = createBytesMessage(1024);

	private static byte[] createBytesMessage(int i) {
		byte[] bs = new byte[i];
		for (int j = 0; j < i; j++) {
			bs[j] = 'A';
		}
		return bs;
	}

}
