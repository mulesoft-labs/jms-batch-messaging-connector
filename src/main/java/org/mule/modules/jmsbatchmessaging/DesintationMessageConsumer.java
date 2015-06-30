package org.mule.modules.jmsbatchmessaging;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mule.api.callback.SourceCallback;
import org.mule.transport.jms.JmsConnector;

public class DesintationMessageConsumer implements Runnable {
	private Integer amountOfMessages;
	private long timeout;
	private SourceCallback callback;
	private String queueName;
	private JmsConnector connector;
	private Boolean isTopic;

	protected transient Log logger = LogFactory.getLog(getClass());

	public DesintationMessageConsumer(Integer amountOfMessages, long timeout,
			SourceCallback callback, JmsConnector connector, String queueName, Boolean isTopic) {
		this.amountOfMessages = amountOfMessages;
		this.timeout = timeout;
		this.callback = callback;
		this.connector = connector;
		this.queueName = queueName;
		this.isTopic = isTopic;
	}

	@Override
	public void run() {
		String threadId = Thread.currentThread().getName();
		logger.debug(String.format("Running worker thread with id -> %s",
				threadId));
		try {
			while (true) {
				if (Thread.interrupted()) {
					throw new InterruptedException();
				}
				Session session;
				Destination destination;
				MessageConsumer consumer;
				synchronized (this) {
					session = connector.getSession(false, false);
					if (isTopic) {
						destination = session.createTopic(queueName);
					} else {
						destination = session.createQueue(queueName);
					}
					consumer = session.createConsumer(destination);
				}

				List<Message> jmsMessagesToAck = new ArrayList<Message>();
				List<String> plainMessages = new ArrayList<>();

				for (Integer i = 0; i < this.amountOfMessages; i++) {
					Message message = consumer.receive(this.timeout);

					if (message != null) {
						jmsMessagesToAck.add(message);
						if (logger.isDebugEnabled()) {
							logger.debug(String.format(
									"Reading message with id -> %s",
									message.getJMSMessageID()));
						}
					} else {
						logger.debug(String
								.format("Current thread (%s) ran out of messages to read from the queue. ",
										threadId));
						break;
					}
				}

				for (Message receivedMessage : jmsMessagesToAck) {
					if (receivedMessage instanceof TextMessage) {
						TextMessage message = (TextMessage) receivedMessage;
						plainMessages.add(message.getText());
					} else if (receivedMessage instanceof BytesMessage) {
						BytesMessage message = (BytesMessage) receivedMessage;
						byte[] bytes = new byte[(int) message.getBodyLength()];
						message.readBytes(bytes);
						plainMessages.add(new String(bytes));
					}
				}
				
				if (plainMessages.size() > 0) {
					Map<String,Object> properties = new HashMap<String,Object>();
					properties.put("session", session);
					properties.put("consumer", consumer);
					properties.put("lastMessage",
							jmsMessagesToAck.get(jmsMessagesToAck.size() - 1));
					callback.process(plainMessages, properties);
				} else {
					consumer.close();
					session.close();					
					logger.debug("Message group is empty, not invoking the flow");
				}
			}
		} catch (Throwable t) {
			logger.error("Error: ", t);
		}
	}

}
