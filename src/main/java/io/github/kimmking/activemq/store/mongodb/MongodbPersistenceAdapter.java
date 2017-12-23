package io.github.kimmking.activemq.store.mongodb;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.scheduler.JobSchedulerStore;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TransactionStore;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongodbPersistenceAdapter implements PersistenceAdapter, BrokerServiceAware {

	private static final Logger LOG = LoggerFactory.getLogger(MongodbPersistenceAdapter.class);

	ConcurrentHashMap<ActiveMQDestination, TopicMessageStore> topicStores = new ConcurrentHashMap<ActiveMQDestination, TopicMessageStore>();
	ConcurrentHashMap<ActiveMQDestination, MessageStore> queueStores = new ConcurrentHashMap<ActiveMQDestination, MessageStore>();

	protected MongoDBHelper helper;
	private WireFormat wireFormat = new OpenWireFormat();

	private String host;
	private int port;
	private String db;
	private String user;
	private String password;

	// private BrokerService brokerService;

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getDb() {
		return db;
	}

	public void setDb(String db) {
		this.db = db;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	@Override
	public void start() throws Exception {
		helper = new MongoDBHelper(host, port, db, user, password, wireFormat);
	}

	@Override
	public void stop() throws Exception {
		if (this.helper != null)
			this.helper.close();
	}
	
	@Override
	public void deleteAllMessages() throws IOException {
		this.helper.removeAllMessages();
	}

	@Override
	public void setBrokerService(BrokerService brokerService) {
		// this.brokerService = brokerService;
	}

	@Override
	public Set<ActiveMQDestination> getDestinations() {
		Set<ActiveMQDestination> set = new HashSet<ActiveMQDestination>();
		List<String> destinations = this.helper.findDestinations();
		for(String dest : destinations){
			set.add(ActiveMQDestination.createDestination(dest, ActiveMQDestination.QUEUE_TYPE));
		}
		return set;
	}

	@Override
	public MessageStore createQueueMessageStore(ActiveMQQueue destination) throws IOException {
		if(LOG.isDebugEnabled())
			LOG.debug("Create QueueMessageStore for destination:[" + destination.getQualifiedName() + "]");
		MongodbMessageStore store = new MongodbMessageStore(destination, wireFormat, helper);
		this.queueStores.put(destination, store);
		return store;
	}

	@Override
	public TopicMessageStore createTopicMessageStore(ActiveMQTopic destination) throws IOException {
		if(LOG.isDebugEnabled())
			LOG.debug("Create TopicMessageStore for destination:[" + destination.getQualifiedName() + "]");
		MongodbTopicMessageStore store = new MongodbTopicMessageStore(destination, wireFormat, helper);
		this.topicStores.put(destination, store);
		return store;
	}

	@Override
	public JobSchedulerStore createJobSchedulerStore() throws IOException, UnsupportedOperationException {
		return null;
	}

	@Override
	public void removeQueueMessageStore(ActiveMQQueue destination) {
		if(LOG.isDebugEnabled())
			LOG.debug("Remove QueueMessageStore for destination:[" + destination.getQualifiedName() + "]");
		this.queueStores.remove(destination);
	}

	@Override
	public void removeTopicMessageStore(ActiveMQTopic destination) {
		if(LOG.isDebugEnabled())
			LOG.debug("Remove TopicMessageStore for destination:[" + destination.getQualifiedName() + "]");
		this.topicStores.remove(destination);
	}

	@Override
	public long size() {
		// ignore
		return 0;
	}

	@Override
	public void setUsageManager(SystemUsage usageManager) {
		// ignore
	}

	@Override
	public void setBrokerName(String brokerName) {
		// ignore
	}

	@Override
	public void setDirectory(File dir) {
		// ignore
	}

	@Override
	public void checkpoint(boolean sync) throws IOException {
		// TODO not supported
	}

	@Override
	public long getLastMessageBrokerSequenceId() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getLastProducerSequenceId(ProducerId id) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void allowIOResumption() {

	}

	public String toString() {
		return "MongodbPersistenceAdapter(" + host + ":" + port + "/" + db + ")";
	}

	// // =========== not supported for tx ==========
	@Override
	public TransactionStore createTransactionStore() throws IOException {
		// TODO not supported
		return new MongodbTransactionStore();
	}

	@Override
	public void beginTransaction(ConnectionContext context) throws IOException {
		// TODO not supported
	}

	@Override
	public void commitTransaction(ConnectionContext context) throws IOException {
		// TODO not supported
	}

	@Override
	public void rollbackTransaction(ConnectionContext context) throws IOException {
		// TODO not supported
	}

	@Override
	public File getDirectory() {
		return null;
	}

}
