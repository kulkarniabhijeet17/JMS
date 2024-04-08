package com.jms.ibmmq.utility;

import com.ibm.msg.client.jms.JmsConnectionFactory;
import com.ibm.msg.client.jms.JmsFactoryFactory;
import com.ibm.msg.client.wmq.WMQConstants;

import javax.jms.*;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

public class JmsGet {
    private static int status = 1;

    public static void main(String[] args) {
        JMSContext context = null;
        Destination destination = null;
        // MessageConsumer consumer = null;
        JMSConsumer consumer = null;

        try {
            InputStream inputStream = JmsPut.class.getClassLoader().getResourceAsStream("config.properties");
            Properties prop = new Properties();

            if(inputStream == null) {
                System.out.println("Unable to find config.properties file");
                return;
            }

            prop.load(inputStream);

            // Create a connection factory
            JmsFactoryFactory ff = JmsFactoryFactory.getInstance(WMQConstants.WMQ_PROVIDER);
            JmsConnectionFactory cf = ff.createConnectionFactory();

            // Set the properties
            cf.setStringProperty(WMQConstants.WMQ_HOST_NAME, prop.getProperty("mq.host"));
            cf.setIntProperty(WMQConstants.WMQ_PORT, Integer.parseInt(prop.getProperty("mq.port")));
            cf.setStringProperty(WMQConstants.WMQ_CHANNEL, prop.getProperty("mq.channel"));
            cf.setStringProperty(WMQConstants.WMQ_QUEUE_MANAGER, prop.getProperty("mq.queueManager"));
            cf.setIntProperty(WMQConstants.WMQ_CONNECTION_MODE, WMQConstants.WMQ_CM_CLIENT);
            cf.setStringProperty(WMQConstants.WMQ_APPLICATIONNAME, prop.getProperty("mq.applicationName"));

            // Create JMS Object
            context = cf.createContext();
            // destination = context.createQueue("queue:///EVENTS.REQUEST");
            destination = context.createQueue(prop.getProperty("mq.createQueue") + prop.getProperty("mq.queue"));
            consumer = context.createConsumer(destination); // autoclosable
            context.close();
            recordSuccess();
        } catch (JMSException e) {
            recordFailure(e);
        } catch (IOException e) {
            recordFailure(e);
        }

        System.exit(status);
    }

    private static void recordSuccess() {
        System.out.println("SUCCESS");
        status = 0;
    }

    private static void recordFailure(Exception e) {
        if (e != null) {
            if (e instanceof JMSException) {
                processJMSException((JMSException) e);
            } else {
                System.out.println("Exception occurred" + Arrays.toString(e.getStackTrace()));
            }
        }
        System.out.println("FAILURE");
        status = -1;
    }

    /**
     * Process JMSException and any associated inner exceptions
     * @param jmsex
     */
    private static void processJMSException(JMSException jmsex) {
        System.out.println("JMSException occurred" + Arrays.toString(jmsex.getStackTrace()));
        Throwable innerException = jmsex.getLinkedException();
        if (innerException != null) {
            System.out.println("Inner exception(s):");
        }
        while (innerException != null) {
            System.out.println(innerException);
            innerException = innerException.getCause();
        }
    }
}
