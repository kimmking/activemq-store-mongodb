package org.qsoft.activemq.store.mongodb;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.ByteSequenceData;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class MongoDBHelper {

	protected static final String MSGS = "ACTIVEMQ_MSGS";
	protected static final String ACKS = "ACTIVEMQ_ACKS";
	protected static final String LOCK = "ACTIVEMQ_LOCK";
	
	protected static final String MSG_COLUMN  = "MSG";
	protected static final String DEST_COLUMN = "CONTAINER";

	private static final Logger LOG = LoggerFactory.getLogger(MongoDBHelper.class);

	Mongo mongo;
	DB db;
	WireFormat wireFormat;

	public MongoDBHelper(String host, int port, String dbName, WireFormat wireFormat) {
		LOG.info("Connect to MongoDB[" + host + ":" + port + ":" + dbName + "]");
		try {
			mongo = new Mongo(host, port);
			db = mongo.getDB(dbName);
		} catch (UnknownHostException e) {
			LOG.error("error host.", e);
			throw new RuntimeException(e);
		} catch (MongoException e) {
			LOG.error("MongoException.", e);
			throw new RuntimeException(e);
		}

		this.wireFormat = wireFormat;
	}

	public DBCollection getMsgsCollection() {
		return this.db.getCollection(MSGS);
	}

	public DBCollection getAcksCollection() {
		return this.db.getCollection(ACKS);
	}

	public DBCollection getLockCollection() {
		return this.db.getCollection(LOCK);
	}

	public Boolean addMessage(Message message) throws IOException {
		BasicDBObject bo = new BasicDBObject();
		MessageId messageId = message.getMessageId();
		// Serialize the Message..
		byte data[];
		try {
			ByteSequence packet = wireFormat.marshal(message);
			data = ByteSequenceData.toByteArray(packet);
		} catch (IOException e) {
			throw IOExceptionSupport.create("Failed to broker message: " + messageId + " in container: " + e, e);
		}
		bo.append("ID", messageId.getBrokerSequenceId());
		bo.append(DEST_COLUMN, message.getDestination().getQualifiedName());
		bo.append("MSGID_PROD", messageId.getProducerId().toString());
		bo.append("MSGID_SEQ", messageId.getProducerSequenceId());
		bo.append("EXPIRATION", message.getExpiration());
		bo.append(MSG_COLUMN, data);
		bo.append("PRIORITY", message.getPriority());
		getMsgsCollection().save(bo);

		return true;
	}

	public static void main(String[] args) {

	}

	public void close() {
		this.mongo.close();
	}

	public Message getMessage(MessageId messageId) throws IOException {

		BasicDBObject bo = new BasicDBObject();
		bo.append("MSGID_PROD", messageId.getProducerId().toString());
		bo.append("MSGID_SEQ", messageId.getProducerSequenceId());
		DBObject o = getMsgsCollection().findOne(bo);
		if (o == null)
			return null;
		byte[] data = (byte[]) o.get(MSG_COLUMN);
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
		BasicDBObject bo = new BasicDBObject();
		bo.append(DEST_COLUMN, destination.getQualifiedName());
		bo.append("MSGID_PROD", messageId.getProducerId().toString());
		bo.append("MSGID_SEQ", messageId.getProducerSequenceId());
		DBObject o = getMsgsCollection().findOne(bo);
		if(o != null){
			Object sequenceId = o.get("ID");
			getMsgsCollection().remove(new BasicDBObject("ID",sequenceId));
			//getMsgsCollection().remove(new BasicDBObject("ID",new BasicDBObject("$lte",sequenceId)));
		}else{
			LOG.error(bo.toString() + " is not found.");
		}
		
	}

	public synchronized void removeAllMessages() {
		getMsgsCollection().drop();
		getAcksCollection().drop();
		getLockCollection().drop();
	}

	public Message findOne() throws IOException {
		DBObject o = getMsgsCollection().findOne();
		if (o == null)
			return null;
		byte[] data = (byte[]) o.get(MSG_COLUMN);
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
		
		BasicDBObject bo = new BasicDBObject();
		bo.append(DEST_COLUMN, container);
		bo.append("ID", new BasicDBObject("$gt",sequenceId));
		
		DBCursor c = getMsgsCollection().find(bo).sort(new BasicDBObject("ID",1)).limit(limit);
		while (c.hasNext()) {
			DBObject o = c.next();
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
		List<String> dists = getMsgsCollection().distinct(DEST_COLUMN);
		return dists;
	}

}
