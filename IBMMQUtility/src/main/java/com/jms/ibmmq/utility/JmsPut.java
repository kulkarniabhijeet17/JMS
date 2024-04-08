package com.jms.ibmmq.utility;

import com.ibm.msg.client.jms.JmsConnectionFactory;
import com.ibm.msg.client.jms.JmsFactoryFactory;
import com.ibm.msg.client.wmq.WMQConstants;

import javax.jms.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

public class JmsPut {
    private static int status = 1;
    
    public static void main(String[] args) {
        JMSContext context = null;
        Destination destination = null;
        // MessageProducer producer = null;
        JMSProducer producer = null;

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
            producer = context.createProducer();

            TextMessage message;
            final File folder = new File(prop.getProperty("mq.inputFilePath"));
            for (final File fileEntry : folder.listFiles()) {
                if (fileEntry.isFile() && fileEntry.getName().endsWith(".input")) {
                    try (InputStream input = new FileInputStream(folder.getPath().replace('\\', '/') + "/" + fileEntry.getName())) {
                        byte[] byt = input.readAllBytes();
                        for (int i = 0; i < byt.length; i++) {
                            System.out.println("byte array" + i + "=");
                            System.out.println(byt[i] + " ");
                        }

                        String plainText = new String(byt, "CP1047");
                        System.out.println("Pushed String = " + plainText);
                        message = context.createTextMessage();
                        message.setText(plainText);
                        System.out.println("Pushed String Length = " + plainText.length());
                        producer.send(destination, message);
                        System.out.println("Sent message:\n " + message);
                    } catch (Exception e) {
                        System.out.println("Exception occurred");
                    }
                }
            }
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
