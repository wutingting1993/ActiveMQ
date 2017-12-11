package com.study.activemq.mq;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by WuTing on 2017/11/9.
 */
public class ActiveMqClient {

	private static final Logger LOG = LoggerFactory.getLogger(ActiveMqClient.class);

	protected ConnectionFactory factory;
	protected Connection connection;
	protected Session session;
	protected String queueName;
	protected MessageConsumer consumer;
	protected MessageProducer producer;
	protected Destination destination;
	protected boolean transacted;
	protected Integer deliveryMode;
	protected Integer acknowledgeMode;
	protected boolean isQueue;

	public ActiveMqClient(String queueName, boolean transacted, boolean isQueue) throws Exception {
		this.queueName = queueName;
		this.transacted = transacted;
		this.isQueue=isQueue;
	}

	public void connect() throws JMSException {
		factory = new ActiveMQConnectionFactory();
		connection = factory.createConnection();
		connection.start();
		session = connection.createSession(transacted, getAcknowledgeMode());
		if (isQueue) {
			createQueue();
		} else {
			createTopic();
		}
		LOG.debug("connect successful !!!");
	}

	public void createQueue() throws JMSException {
		destination = session.createQueue(queueName);
	}

	public void createTopic() throws JMSException {
		destination = session.createTopic(queueName);
	}

	public void createConsumer() throws JMSException {
		consumer = session.createConsumer(destination);
		LOG.debug("create Consumer successful !!!");
	}

	public void createProducer() throws Exception {
		producer = session.createProducer(destination);
		LOG.debug("create Producer successful !!!");
	}

	public void setDeliveryMode() throws Exception {
		producer.setDeliveryMode(getDeliveryMode());
	}

	public void setMessageListener(MessageListener listener) throws Exception {
		consumer.setMessageListener(listener);
	}

	public void commit() throws Exception {
		session.commit();
	}

	public void close() throws Exception {
		session.close();
		connection.close();
	}

	public Integer getAcknowledgeMode() {
		if (acknowledgeMode == null) {
			acknowledgeMode = Session.AUTO_ACKNOWLEDGE;
		}
		return acknowledgeMode;
	}

	public void setAcknowledgeMode(Integer acknowledgeMode) {
		this.acknowledgeMode = acknowledgeMode;
	}

	public Integer getDeliveryMode() {
		if (deliveryMode == null) {
			deliveryMode = DeliveryMode.NON_PERSISTENT;
		}
		return deliveryMode;
	}

	public void setDeliveryMode(Integer deliveryMode) {
		this.deliveryMode = deliveryMode;
	}
}
