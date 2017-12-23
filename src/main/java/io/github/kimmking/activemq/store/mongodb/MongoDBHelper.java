package io.github.kimmking.activemq.store.mongodb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.*;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.ByteSequenceData;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.wireformat.WireFormat;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

public class MongoDBHelper {

	protected static final String MSGS = "ACTIVEMQ_MSGS";
	protected static final String ACKS = "ACTIVEMQ_ACKS";
	protected static final String LOCK = "ACTIVEMQ_LOCK";
	
	protected static final String MSG_COLUMN  = "MSG";
	protected static final String DEST_COLUMN = "CONTAINER";

	private static final Logger LOG = LoggerFactory.getLogger(MongoDBHelper.class);

	MongoClient mongoClient;
	MongoDatabase mongoDatabase;
	WireFormat wireFormat;


	public MongoDBHelper(String host, int port, String dbName,String userName,String password, WireFormat wireFormat) {
		LOG.info("Connect to MongoDB[" + host + ":" + port + ":" + dbName + "]");

		ServerAddress serverAddress = new ServerAddress(host,port);
		List<ServerAddress> addrs = new ArrayList<ServerAddress>();
		addrs.add(serverAddress);
		List<MongoCredential> credentials = new ArrayList<MongoCredential>();

		if(!StringUtils.isEmpty(userName)) {
			MongoCredential credential = MongoCredential.createScramSha1Credential(userName, dbName, password.toCharArray());
			credentials.add(credential);
		}

		this.mongoClient = new MongoClient(addrs,credentials);
		this.mongoDatabase = mongoClient.getDatabase(dbName);

		this.wireFormat = wireFormat;
	}

	public MongoCollection<Document> getMsgsCollection() {
		return this.mongoDatabase.getCollection(MSGS);
	}

	public MongoCollection<Document> getAcksCollection() {
		return this.mongoDatabase.getCollection(ACKS);
	}

	public MongoCollection<Document> getLockCollection() {
		return this.mongoDatabase.getCollection(LOCK);
	}

	public Boolean addMessage(Message message) throws IOException {
		BasicDBObject bo = new BasicDBObject();
		MessageId messageId = message.getMessageId();
		// Serialize the Message..

		List<Document> documents = new ArrayList<Document>();


		byte data[];
		try {
			ByteSequence packet = wireFormat.marshal(message);
			data = ByteSequenceData.toByteArray(packet);
		} catch (IOException e) {
			throw IOExceptionSupport.create("Failed to broker message: " + messageId + " in container: " + e, e);
		}
		Document document = new Document("ID", messageId.getBrokerSequenceId())
		.append(DEST_COLUMN, message.getDestination().getQualifiedName())
		.append("MSGID_PROD", messageId.getProducerId().toString())
		.append("MSGID_SEQ", messageId.getProducerSequenceId())
		.append("EXPIRATION", message.getExpiration())
		.append(MSG_COLUMN, data)
		.append("PRIORITY", message.getPriority());

		documents.add(document);
		getMsgsCollection().insertOne(document);

		return true;
	}

	public static void main(String[] args) {

	}

	public void close() {
		this.mongoClient.close();
	}

	public Message getMessage(MessageId messageId) throws IOException {

//		BasicDBObject bo = new BasicDBObject();
//		bo.append("MSGID_PROD", messageId.getProducerId().toString());
//		bo.append("MSGID_SEQ", messageId.getProducerSequenceId());
		FindIterable<Document> findIterable = getMsgsCollection().find(Filters.and(
				Filters.eq("MSGID_SEQ", messageId.getProducerSequenceId()),
				Filters.eq("MSGID_PROD",messageId.getProducerId().toString())
				)).limit(1);
		Document document = findIterable.first();
		if (document == null)
			return null;
		byte[] data = (byte[]) document.get(MSG_COLUMN);
		if (data == null)
			return null;

		Message answer = null;
		try {
			answer = (Message) wireFormat.unmarshal(new ByteSequence(data));
		} catch (IOException e) {
			throw IOExceptionSupport.create("Failed to broker message: " + messageId + " in container: " + e, e);
		}
		return answer;
	}

	public int count() {
		return (int) getMsgsCollection().count();
	}

	public synchronized void removeMessage(ActiveMQDestination destination, MessageAck ack) {
		MessageId messageId = ack.getLastMessageId();
		ack.getDestination();

		FindIterable<Document> findIterable = getMsgsCollection().find(Filters.and(
				Filters.eq(DEST_COLUMN, destination.getQualifiedName()),
				Filters.eq("MSGID_SEQ", messageId.getProducerSequenceId()),
				Filters.eq("MSGID_PROD",messageId.getProducerId().toString())
		)).limit(1);
		Document document = findIterable.first();
		if(document != null){
			Object sequenceId = document.get("ID");
			getMsgsCollection().deleteOne(Filters.eq("ID",sequenceId));
					//.remove(new BasicDBObject("ID",sequenceId));
			//getMsgsCollection().remove(new BasicDBObject("ID",new BasicDBObject("$lte",sequenceId)));
		}else{
			LOG.error(document.toString() + " is not found.");
		}
		
	}

	public synchronized void removeAllMessages() {
		getMsgsCollection().drop();
		getAcksCollection().drop();
		getLockCollection().drop();
	}

	public Message findOne() throws IOException {
		FindIterable<Document> findIterable = getMsgsCollection().find().limit(1);
		Document document = findIterable.first();
		if (document == null)
			return null;
		byte[] data = (byte[]) document.get(MSG_COLUMN);
		if (data == null)
			return null;

		Message answer = null;
		try {
			answer = (Message) wireFormat.unmarshal(new ByteSequence(data));
		} catch (IOException e) {
			throw IOExceptionSupport.create("Failed to broker message in container: " + e, e);
		}
		return answer;
	}

	public List<Message> find(int limit, String container, long sequenceId) throws IOException {
		List<Message> msgs = new ArrayList<Message>(limit);
		
//		BasicDBObject bo = new BasicDBObject();
//		bo.append(DEST_COLUMN, container);
//		bo.append("ID", new BasicDBObject("$gt",sequenceId));

		FindIterable<Document> findIterable = getMsgsCollection().find(Filters.and(
				Filters.eq(DEST_COLUMN, container),
				Filters.gt("ID", sequenceId)
		)).sort(Sorts.ascending("ID")).limit(limit);
		
		//DBCursor c = getMsgsCollection().find(bo).sort(new BasicDBObject("ID",1)).limit(limit);
		while (findIterable.iterator().hasNext()) {
			Document o = findIterable.iterator().next();
			if (o == null)
				return null;
			byte[] data = (byte[]) o.get(MSG_COLUMN);
			if (data == null)
				return null;
			Message answer = null;
			try {
				answer = (Message) wireFormat.unmarshal(new ByteSequence(data));
				msgs.add(answer);
			} catch (IOException e) {
				throw IOExceptionSupport.create("Failed to broker message in container: " + e, e);
			}
		}
		return trim(msgs);
	}

	private List<Message> trim(List<Message> msgs2) {
		if(msgs2 == null || msgs2.size() == 0)
		return null;
		Message m = msgs2.get(msgs2.size()-1);
		while(m==null && msgs2.size() > 0){
			m = msgs2.remove(msgs2.size()-1);
		}
		return msgs2;
	}

	@SuppressWarnings("unchecked")
	public List<String> findDestinations() {
		List<String> dists = new ArrayList<String>();
		DistinctIterable<String>  iterable = getMsgsCollection().distinct(DEST_COLUMN,String.class);
		while (iterable.iterator().hasNext()) {
			dists.add(iterable.iterator().next());
		}
		return dists;
	}

}
