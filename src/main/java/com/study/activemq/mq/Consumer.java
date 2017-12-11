package com.study.activemq.mq;

import javax.jms.BytesMessage;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.study.activemq.model.User;

/**
 * Created by WuTing on 2017/11/9.
 */
public class Consumer extends ActiveMqClient {
	private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

	public Consumer(String queueName, boolean transacted, boolean isQueue) throws Exception {
		super(queueName, transacted, false);
	}

	public Message getMessage() throws Exception {
		return consumer.receive(100000);
	}

	public TextMessage getTextMessage() throws Exception {
		TextMessage message = (TextMessage)getMessage();
		return (TextMessage)consumer.receive(100000);
	}

	public ObjectMessage getObjectMessage() throws Exception {
		return (ObjectMessage)consumer.receive(100000);
	}

	public StreamMessage getStreamMessage() throws Exception {
		return (StreamMessage)consumer.receive(100000);
	}

	public BytesMessage getBytesMessage() throws Exception {
		return (BytesMessage)consumer.receive(100000);
	}

	public MapMessage getMapMessage() throws Exception {
		return (MapMessage)consumer.receive(100000);
	}

	public static void main(String[] args) throws Exception {
		//testQueue();
		testTopic();
	}

	private static void testTopic() throws Exception {
		Consumer consumer = new Consumer("test-topic", false, true);
		consumer.setAcknowledgeMode(Session.AUTO_ACKNOWLEDGE);
		consumer.connect();
		consumer.createConsumer();
		LOG.debug("[x] receive message：\n");
		consumer.setMessageListener(new MessageListener() {
			public void onMessage(Message message) {
				try {
					User user = (User)((ObjectMessage)message).getObject();
					if (null != message) {
						System.out.println("收到消息" + user.getName());
					}
				} catch (Exception e) {
					LOG.error(e.getMessage(), e);
				}
			}
		});
	}

	private static void testQueue() throws Exception {
		Consumer consumer = new Consumer("hello", true, true);
		consumer.connect();
		consumer.createConsumer();
		LOG.debug("[x] receive message：\n");
		while (true) {
			TextMessage message = (TextMessage)consumer.getMessage();
			LOG.debug("message received：" + message.getText());
		}
	}

}
