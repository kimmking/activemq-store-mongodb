package org.qsoft.activemq.store.mongodb;

import java.io.IOException;

import org.apache.activemq.command.TransactionId;
import org.apache.activemq.store.TransactionRecoveryListener;
import org.apache.activemq.store.TransactionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongodbTransactionStore implements TransactionStore {

	private static final Logger LOG = LoggerFactory
			.getLogger(MongodbMessageStore.class);

	@Override
	public void start() throws Exception {
		LOG.debug("MongodbTransactionStore.start");

	}

	@Override
	public void stop() throws Exception {
		LOG.debug("MongodbTransactionStore.stop");

	}

	@Override
	public void prepare(TransactionId txid) throws IOException {
		LOG.debug("MongodbTransactionStore.prepare");

	}

	@Override
	public void commit(TransactionId txid, boolean wasPrepared,
			Runnable preCommit, Runnable postCommit) throws IOException {
		LOG.debug("MongodbTransactionStore.commit");

	}

	@Override
	public void rollback(TransactionId txid) throws IOException {
		LOG.debug("MongodbTransactionStore.rollback");

	}

	@Override
	public void recover(TransactionRecoveryListener listener)
			throws IOException {
		LOG.debug("MongodbTransactionStore.recover");

	}

}
