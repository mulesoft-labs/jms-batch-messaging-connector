/**
 * (c) 2003-2015 MuleSoft, Inc. The software in this package is published under the terms of the CPAL v1.0 license,
 * a copy of which has been included with this distribution in the LICENSE.md file.
 */

package org.mule.modules.jmsbatchmessaging;

import java.util.List;
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
import org.mule.api.annotations.param.Default;
import org.mule.api.annotations.param.Optional;
import org.mule.api.callback.SourceCallback;
import org.mule.api.context.MuleContextAware;
import org.mule.transport.jms.JmsConnector;

import javax.inject.Inject;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

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

    MuleContext muleContext;

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
    public void consume(String destinationName, Boolean isTopic, int batchSize, long timeout,
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
                        timeout, callback, connector, destinationName, isTopic));
            }

            executorService.shutdown();

            try {
                executorService.awaitTermination(Long.MAX_VALUE,
                        TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
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

        session.close();
        consumer.close();
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


}