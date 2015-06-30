package org.mule.modules.jmsbatchmessaging;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mule.api.MuleContext;
import org.mule.api.callback.SourceCallback;
import org.mule.transport.jms.JmsConnector;
import org.mule.transport.jms.JmsConstants;
import org.mule.transport.jms.JmsMessageUtils;
import org.mule.transport.jms.transformers.JMSMessageToObject;

import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DestinationMessageConsumer implements Runnable {
    private Integer amountOfMessages;
    private long timeout;
    private SourceCallback callback;
    private String queueName;
    private JmsConnector connector;
    private Boolean isTopic;
    private MuleContext muleContext;

    protected transient Log logger = LogFactory.getLog(getClass());

    public DestinationMessageConsumer(MuleContext muleContext, Integer amountOfMessages, long timeout,
                                      SourceCallback callback, JmsConnector connector,
                                      String queueName, Boolean isTopic) {
        this.amountOfMessages = amountOfMessages;
        this.timeout = timeout;
        this.callback = callback;
        this.connector = connector;
        this.queueName = queueName;
        this.isTopic = isTopic;
        this.muleContext = muleContext;
    }

    @Override
    public void run() {
        String threadId = Thread.currentThread().getName();
        logger.debug(String.format("Running worker thread with id -> %s",
                threadId));
        try {
            while (!Thread.interrupted() && connector.isStarted()) {
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

                List<Message> jmsMessagesToAck = new ArrayList<>();
                List<Object> plainMessages = new ArrayList<>();

                for (Integer i = 0; i < this.amountOfMessages; i++) {
                    Message message = consumer.receive(this.timeout);

                    if (message != null) {
                        jmsMessagesToAck.add(message);
                        logger.debug(String.format(
                                "Reading message with id -> %s",
                                message.getJMSMessageID()));
                    } else {
                        logger.debug(String
                                .format("Current thread (%s) ran out of messages to read from the queue. ",
                                        threadId));
                        break;
                    }
                }

                for (Message receivedMessage : jmsMessagesToAck) {
                    plainMessages.add(JmsMessageUtils.toObject(receivedMessage, JmsConstants.JMS_SPECIFICATION_11,
                            muleContext.getConfiguration().getDefaultEncoding()
                    ));
                }

                if (plainMessages.size() > 0) {
                    Map<String, Object> properties = new HashMap<>();
                    properties.put("session", session);
                    properties.put("consumer", consumer);
                    properties.put("messages", jmsMessagesToAck);
                    callback.process(plainMessages, properties);
                }
            }
        } catch (Throwable t) {
            logger.error("Error: ", t);
        }
    }

}
