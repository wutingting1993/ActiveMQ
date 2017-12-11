package com.study.activemq.mq;

import java.io.Serializable;
import java.util.List;

import javax.jms.DeliveryMode;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.study.activemq.model.User;

/**
 * Created by WuTing on 2017/11/9.
 */
public class Producer<T extends Serializable> extends ActiveMqClient {
	private static final Logger LOG = LoggerFactory.getLogger(Producer.class);

	public Producer(String queueName, boolean transacted, boolean isQueue) throws Exception {
		super(queueName, transacted, isQueue);
	}

	public void sendTextMessge(String message) throws Exception {
		LOG.debug("send：" + message);
		producer.send(session.createTextMessage(message));
	}

	public void sendTextMessges(List<String> messages) throws Exception {
		for (String message : messages) {
			LOG.debug("send：" + message);
			producer.send(session.createTextMessage(message));
		}
	}

	public void sendObjectMessage(T message) throws Exception {
		LOG.debug("send：" + message);
		producer.send(session.createObjectMessage(message));
	}

	public static void main(String[] args) throws Exception {
		//testQueue();
		testTopic();

	}

	private static void testTopic() throws Exception {
		System.out.println("[x] Send message：");
		Producer producer = new Producer("test-topic", false, false);
		producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		producer.setAcknowledgeMode(Session.AUTO_ACKNOWLEDGE);
		producer.connect();
		producer.createProducer();
		User user = new User();
		user.setAge(12);
		for (int i = 0; i < 100; i++) {
			Thread.sleep(1000);
			user.setName("小黄" + i);
			producer.sendObjectMessage(user);
		}

		producer.commit();
		producer.close();
	}

	private static void testQueue() throws Exception {
		System.out.println("[x] Send message：");
		Producer producer = new Producer("hello", false, true);
		producer.connect();
		producer.createProducer();
		producer.sendTextMessge("你好呀， activeMQ");

		User user = new User();
		user.setName("zhangsan");
		user.setAge(12);
		producer.sendObjectMessage(user);

		producer.commit();
		producer.close();
	}
}
