package kaha;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.xbean.XBeanBrokerFactory;

public class TestServerKaha {

	private static final String url = "tcp://localhost:61616";;
	private static final String QUEUE_NAME = "kk.kaha";
	//private static final String TOPIC_NAME = "mysql";

	public static void main(String[] args) throws Exception {

		XBeanBrokerFactory factory = new XBeanBrokerFactory();
		BrokerService broker = factory.createBroker(new URI("activemq-kaha.xml"));

		// BrokerService broker = new BrokerService();
		// broker.setBrokerName("kk");
		// broker.setPersistent(false);
		// broker.setUseJmx(true);
		broker.start();
		
		while(true){
			Thread.sleep(1000);
		}

//		Connection connection = null;
//
//		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
//		connection = connectionFactory.createConnection();
//
//		connection.start();
//		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
//		Destination queue = session.createQueue(QUEUE_NAME);
//		Destination topic = session.createTopic(TOPIC_NAME);
//		MessageProducer producer = session.createProducer(queue);
//		MessageConsumer receiver = session.createConsumer(queue);
//
//		MessageProducer producer1 = session.createProducer(topic);
//		MessageConsumer receiver1 = session.createConsumer(topic);
//
//		try {
//			BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
//			while (true) {
//				String line = reader.readLine();
//				if (line == null)
//					continue;
//				if (line.length() < 5) {
//					System.out.println(" error command: " + line);
//					continue;
//				}
//
//				if ("quit".equalsIgnoreCase(line)) {
//					System.out.println(line);
//					break;
//				}
//
//				String[] sline = line.split(" ");
//
//				String command = sline[0];
//				String content = "";
//				long ct = 10;
//
//				if (sline.length > 1)
//					content = sline[1];
//				if (sline.length > 2)
//					ct = Integer.parseInt(sline[2]);
//
//				if ("send".equalsIgnoreCase(command)) {
//					TextMessage message = session.createTextMessage(content);
//					producer.send(message);
//					System.out.println(" send message: " + message);
//				} else if ("recv".equalsIgnoreCase(command)) {
//					try {
//						Message message = receiver.receive(1000);
//						System.out.println(" receive message: " + message);
//					} catch (Exception e) {
//						System.out.println(" error: receive message ");
//						e.printStackTrace();
//					}
//				} else if ("sent".equalsIgnoreCase(command)) {
//					TextMessage message = session.createTextMessage(content);
//					message.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
//					for (int i = 0; i < ct; i++) {
//						producer1.send(message);
//						System.out.println(i + " send message: " + message);
//					}
//				} else if ("rect".equalsIgnoreCase(command)) {
//					try {
//						for (int i = 0; i < ct; i++) {
//							Message message = receiver1.receive();
//							System.out.println(i +" receive message: " + message);
//						}
//					} catch (Exception e) {
//						System.out.println(" error: receive message ");
//						e.printStackTrace();
//					}
//				} else {
//					System.out.println(" error command: " + line);
//				}
//			}
//
//			receiver.close();
//			producer.close();
//
//			receiver1.close();
//			producer1.close();
//
//			session.close();
//
//		} catch (Exception e) {
//			e.printStackTrace();
//		} finally {
//			connection.close();
//		}
//
//		broker.stop();
	}

}
