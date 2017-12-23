package io.github.kimmking.activemq.store.mongodb;

import java.io.IOException;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStoreSubscriptionStatistics;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongodbTopicMessageStore extends MongodbMessageStore implements TopicMessageStore {

	private static final Logger LOG = LoggerFactory
			.getLogger(MongodbTopicMessageStore.class);

	public MongodbTopicMessageStore(ActiveMQDestination destination,
			WireFormat wireFormat, MongoDBHelper helper) {
		super(destination, wireFormat, helper);
	}

	@Override
	public void acknowledge(ConnectionContext context, String clientId,
			String subscriptionName, MessageId messageId, MessageAck ack)
			throws IOException {
		if(LOG.isDebugEnabled())
			LOG.debug("MongodbTopicMessageStore.acknowledge: " + clientId + ","
				+ subscriptionName + "," + messageId);

	}

	@Override
	public void deleteSubscription(String clientId, String subscriptionName)
			throws IOException {
		LOG.debug("MongodbTopicMessageStore.deleteSubscription");

	}

	@Override
	public void recoverSubscription(String clientId, String subscriptionName,
			MessageRecoveryListener listener) throws Exception {
		LOG.debug("MongodbTopicMessageStore.recoverSubscription");
	}

	@Override
	public void recoverNextMessages(String clientId, String subscriptionName,
			int maxReturned, MessageRecoveryListener listener) throws Exception {
		LOG.debug("MongodbTopicMessageStore.recoverNextMessages");
	}

	@Override
	public void resetBatching(String clientId, String subscriptionName) {
		LOG.debug("MongodbTopicMessageStore.resetBatching");
	}

	@Override
	public int getMessageCount(String clientId, String subscriberName)
			throws IOException {
		LOG.debug("MongodbTopicMessageStore.getMessageCount");
		return 0;
	}

	@Override
	public long getMessageSize(String s, String s1) throws IOException {
		return 0;
	}

	@Override
	public MessageStoreSubscriptionStatistics getMessageStoreSubStatistics() {
		return null;
	}

	@Override
	public SubscriptionInfo lookupSubscription(String clientId,
			String subscriptionName) throws IOException {
		LOG.debug("MongodbTopicMessageStore.lookupSubscription");
		return null;
	}

	@Override
	public SubscriptionInfo[] getAllSubscriptions() throws IOException {
		LOG.debug("MongodbTopicMessageStore.getAllSubscriptions");
		return new SubscriptionInfo[0];
	}

	@Override
	public void addSubscription(SubscriptionInfo subscriptionInfo, boolean retroactive) throws IOException {
		LOG.debug("MongodbTopicMessageStore.addSubsciption");
	}



}
