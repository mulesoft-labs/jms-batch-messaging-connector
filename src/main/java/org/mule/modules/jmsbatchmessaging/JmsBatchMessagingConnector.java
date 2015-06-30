/**
 * (c) 2003-2015 MuleSoft, Inc. The software in this package is published under the terms of the CPAL v1.0 license,
 * a copy of which has been included with this distribution in the LICENSE.md file.
 */

package org.mule.modules.jmsbatchmessaging;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mule.api.MuleContext;
import org.mule.api.MuleMessage;
import org.mule.api.annotations.Connector;
import org.mule.api.annotations.Configurable;
import org.mule.api.annotations.Processor;
import org.mule.api.annotations.Source;
import org.mule.api.annotations.lifecycle.Stop;
import org.mule.api.annotations.param.Default;
import org.mule.api.annotations.param.Optional;
import org.mule.api.annotations.param.Payload;
import org.mule.api.callback.SourceCallback;
import org.mule.api.context.MuleContextAware;
import org.mule.transport.jms.JmsConnector;

import javax.inject.Inject;
import javax.jms.*;

/**
 * Anypoint Connector
 *
 * @author MuleSoft, Inc.
 */
@Connector(name = "jms-batch-messaging", friendlyName = "Batch Messaging (JMS)")
public class JmsBatchMessagingConnector implements MuleContextAware {

    protected transient Log logger = LogFactory.getLog(getClass());

    /**
     * Configurable
     */
    @Configurable
    JmsConnector connector;

    /**
     * Configurable
     */
    @Configurable
    @Default("1")
    Integer amountOfThreads;

    /**
     * Configurable
     */
    @Configurable
    @Default("1")
    Boolean isTransactional;

    MuleContext muleContext;

    Timer sendTimer = new Timer();

    Timer timeOutTimer;

    Map<String, List<String>> sendMessageBuffer = new ConcurrentHashMap<>();


    /**
     * Custom processor
     * <p/>
     * {@sample.xml ../../../doc/db-batch-messaging-connector.xml.sample
     * db-batch-messaging:batch-consume}
     *
     * @param callback        Comment for callback
     * @param destinationName Comment for queueName
     * @param batchSize       Comment for batchSize
     * @param timeout         Comment for timeout
     * @throws Exception Comment for Exception
     */
    @Source
    public void consume(String destinationName, Boolean isTopic,
                        int batchSize, long timeout,
                        final SourceCallback callback) throws Exception {
        Lock lock = null;

        try {
            lock = muleContext.getLockFactory().createLock("JMS_BATCH_MESSAGING_CONSUMER_LOCK");
            lock.lock();

            logger.debug(String.format(
                    "Starting batch (size=%s) processing with %s threads",
                    batchSize, this.amountOfThreads));

            ExecutorService executorService = Executors
                    .newFixedThreadPool(this.amountOfThreads);
            for (int i = 0; i < this.amountOfThreads; i++) {
                executorService.execute(new DestinationMessageConsumer(muleContext, batchSize,
                        timeout, callback, connector, destinationName, isTopic, isTransactional));
            }

            executorService.shutdown();

            try {
                executorService.awaitTermination(Long.MAX_VALUE,
                        TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                logger.debug("Thread interrupted");
            }

        } finally {
            if (lock != null) {
                lock.unlock();
            }
        }
    }

    @Processor
    @Inject
    public void acknowledge(MuleMessage muleMessage, @Optional Message message)
            throws Exception {

        if (message != null) {
            message.acknowledge();

        } else {
            List<Message> messages = muleMessage.getInboundProperty("messages");
            for (Message eachMessage : messages) {
                eachMessage.acknowledge();
            }
        }
    }

    @Processor
    @Inject
    public void complete(MuleMessage muleMessage) throws Exception {
        Session session = muleMessage.getInboundProperty("session");
        MessageConsumer consumer = muleMessage.getInboundProperty("consumer");

        Boolean isTransactional = muleMessage.getInboundProperty("transactional");

        if (isTransactional) {
            session.commit();
        }
        session.close();
        consumer.close();
    }


    /**
     * Custom processor
     * <p/>
     * {@sample.xml ../../../doc/db-batch-messaging-connector.xml.sample
     * db-batch-messaging:batch-send}
     *
     * @param payload   Comment for payload
     * @param destinationName Comment for queueName
     * @param batchSize Comment for batchSize
     * @return Some string
     * @throws Exception Comment for Exception
     */
    @SuppressWarnings({"unchecked"})
    @Processor
    public synchronized void send(@Payload Object payload,
                                       String destinationName, int batchSize, int sendTimeout, Boolean isTopic) throws Exception {

        List<String> queueMessages = getListOfMessages(sendMessageBuffer,
                destinationName);
        if (timeOutTimer != null) {
            timeOutTimer.cancel();
        }

        if (payload instanceof Collection) {
            Collection<String> messages = (Collection<String>) payload;
            queueMessages.addAll(messages);
        } else if (payload instanceof String) {
            queueMessages.add((String) payload);
        }

        if (queueMessages.size() >= batchSize) {
            sendTimer.schedule(new MessageSender().setParams(sendMessageBuffer,
                    destinationName, connector, isTopic), 1);
        } else {
            timeOutTimer = new Timer();
            timeOutTimer.schedule(new MessageSender().setParams(
                    sendMessageBuffer, destinationName, connector, isTopic), sendTimeout);
        }
    }


    public List<String> getListOfMessages(Map<String, List<String>> map,
                                          String queue) {
        List<String> messages = map.get(queue);
        if (messages == null) {
            messages = Collections.synchronizedList(new ArrayList<String>());
            map.put(queue, messages);
        }
        return messages;
    }


    public JmsConnector getConnector() {
        return connector;
    }

    public void setConnector(JmsConnector connector) {
        this.connector = connector;
    }

    public Integer getAmountOfThreads() {
        return amountOfThreads;
    }

    public void setAmountOfThreads(Integer amountOfThreads) {
        this.amountOfThreads = amountOfThreads;
    }

    public MuleContext getMuleContext() {
        return muleContext;
    }

    public void setMuleContext(MuleContext muleContext) {
        this.muleContext = muleContext;
    }


    public Boolean getIsTransactional() {
        return isTransactional;
    }

    public void setIsTransactional(Boolean isTransactional) {
        this.isTransactional = isTransactional;
    }

    @Stop
    public void stopConnector() {
        sendTimer.cancel();
        if (timeOutTimer != null) {
            timeOutTimer.cancel();
        }
    }

    class MessageSender extends TimerTask {
        Map<String, List<String>> sendMessageBuffer;
        String queueName;
        JmsConnector connector;
        Boolean isTopic;

        public MessageSender setParams(
                Map<String, List<String>> sendMessageBuffer, String queueName,
                JmsConnector connector, Boolean isTopic) {
            this.sendMessageBuffer = sendMessageBuffer;
            this.queueName = queueName;
            this.connector = connector;
            this.isTopic = isTopic;
            return this;
        }

        @Override
        public void run() {
            try {
                sendMessages(sendMessageBuffer, queueName,
                        connector.getSession(isTransactional, isTopic));
            } catch (java.lang.InterruptedException ie) {
                // do nothing
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        public void sendMessages(Map<String, List<String>> messageMap,
                                 String queueName, Session session) throws Exception {
            MessageProducer producer = null;
            try {
                Destination destination = session.createQueue(queueName);
                producer = session.createProducer(destination);

                List<String> messages = messageMap.put(queueName,
                        Collections.synchronizedList(new ArrayList<String>()));

                for (String message : messages) {
                    logger.debug("Sending message: " + message);
                    producer.send(session.createTextMessage(message));
                }
                messages.clear();
                if (isTransactional) {
                    session.commit();
                }
                session.close();
            } finally {
                if (session != null) {
                    session.close();
                }
                if (producer != null) {
                    producer.close();
                }
            }
        }

    }


}