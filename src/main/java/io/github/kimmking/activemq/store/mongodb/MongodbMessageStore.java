package io.github.kimmking.activemq.store.mongodb;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.AbstractMessageStore;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongodbMessageStore extends AbstractMessageStore {

	protected AtomicLong lastRecoveredSequenceId = new AtomicLong(-1);
	protected final WireFormat wireFormat;
	protected final MongoDBHelper helper;
	private static final Logger LOG = LoggerFactory.getLogger(MongodbMessageStore.class);

	public MongodbMessageStore(ActiveMQDestination destination, WireFormat wireFormat, MongoDBHelper helper) {
		super(destination);
		this.wireFormat = wireFormat;
		this.helper = helper;
	}

	@Override
	public void addMessage(ConnectionContext context, Message message) throws IOException {
		if(LOG.isInfoEnabled())
			LOG.info("MongodbMessageStore.addMessage: " + message);
		this.helper.addMessage(message);
	}

	@Override
	public Message getMessage(MessageId identity) throws IOException {
		if(LOG.isInfoEnabled())
			LOG.info("MongodbMessageStore.getMessage:{0}", identity);
		return this.helper.getMessage(identity);
	}

	@Override
	public void removeMessage(ConnectionContext context, MessageAck ack) throws IOException {
		if(LOG.isInfoEnabled())
			LOG.info("MongodbMessageStore.removeMessage: " + context + "," + ack);
		this.helper.removeMessage(this.getDestination(), ack);
	}

	@Override
	public void removeAllMessages(ConnectionContext context) throws IOException {
		if(LOG.isInfoEnabled())
			LOG.info("MongodbMessageStore.removeAllMessages");
		this.helper.removeAllMessages();
	}

	@Override
	public void recover(MessageRecoveryListener container) throws Exception {
		if(LOG.isInfoEnabled())
			LOG.info("MongodbMessageStore.recover: " + container);
		// TODO ? what is this
	}

	@Override
	public int getMessageCount() throws IOException {
		LOG.info("MongodbMessageStore.getMessageCount:"+this.helper.count());
		return this.helper.count();
	}

	@Override
	public void resetBatching() {
		LOG.info("MongodbMessageStore.resetBatching");
	}

	@Override
	public void recoverNextMessages(int maxReturned, MessageRecoveryListener listener) throws Exception {
		if(LOG.isInfoEnabled())
			LOG.info("MongodbMessageStore.recoverNextMessages: " + maxReturned + " from " + lastRecoveredSequenceId.get());
		//long start = System.currentTimeMillis();
		List<Message> msgs = this.helper.find(maxReturned, this.getDestination().getQualifiedName(), lastRecoveredSequenceId.get());
		//long end1 = System.currentTimeMillis();
		
		if(msgs != null) {
			for (Message message : msgs) {
				listener.recoverMessage(message);
				lastRecoveredSequenceId.set(message.getMessageId().getBrokerSequenceId());
			}
			if(LOG.isInfoEnabled())
				LOG.info("MongodbMessageStore.recoverNextMessages: " + msgs.size() + " ~ " + this.getDestination().getQualifiedName());
		}
		else{
			if(LOG.isInfoEnabled())
				LOG.info("MongodbMessageStore.recoverNextMessages: NONE ~ " + this.getDestination().getQualifiedName());
		}
//		long end = System.currentTimeMillis();
//		System.out.println((end1-start)+" " + (end-end1));

	}

}
