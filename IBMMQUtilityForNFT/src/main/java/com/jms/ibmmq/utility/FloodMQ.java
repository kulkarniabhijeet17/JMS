package com.jms.ibmmq.utility;

import com.ibm.msg.client.jms.JmsConnectionFactory;
import com.ibm.msg.client.jms.JmsFactoryFactory;
import com.ibm.msg.client.wmq.WMQConstants;

import javax.jms.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.*;

public class FloodMQ {
    private static final Logger logger = Logger.getLogger(FloodMQ.class.getName());
    private static int status = 1;

    public static void main(String[] args) {
        String textFilePath = System.getProperty("user.dir") + "/src/main/resources/NFTLogs/" + LocalDate.now();
        String textFileName = "MQ_Flood_Push_Log_" + LocalDate.now();
        textFileName = textFileName.replace(":", "");

        String logFileFullName = textFilePath + "/" + textFileName + ".log";
        File textFile = new File(logFileFullName);
        if (!textFile.getParentFile().exists()) {
            textFile.getParentFile().mkdirs();
        }

        logger.setLevel(Level.ALL);
        ConsoleHandler consoleHandler = new ConsoleHandler();
        consoleHandler.setFormatter(new CustomFormatter());
        logger.addHandler(consoleHandler);
        try {
            FileHandler fileHandler = new FileHandler(logFileFullName);
            fileHandler.setFormatter(new CustomFormatter());
            logger.addHandler(fileHandler);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        JMSContext context = null;
        Destination destination = null;
        // MessageProducer producer = null;
        JMSProducer producer = null;
        Timestamp timestamp = new Timestamp(0L);

        try {
            InputStream inputStream = new FileInputStream(System.getProperty("user.dir") + "/src/main/resources/config.properties");
            Properties prop = new Properties();

            if (inputStream == null) {
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
            cf.setBooleanProperty(WMQConstants.USER_AUTHENTICATION_MQCSP, Boolean.parseBoolean(prop.getProperty("mq.userAuthenticationMQCSP")));

            // Create JMS Object
            context = cf.createContext();
            // destination = context.createQueue("queue:///EVENTS.REQUEST");
            destination = context.createQueue(prop.getProperty("mq.createQueue") + prop.getProperty("mq.queue"));
            producer = context.createProducer();

            TextMessage message = null;
            String plainText;
            String path = System.getProperty("user.dir") + "/" + prop.getProperty("mq.inputFilePath");
            final File folder = new File(path);
            for (final File fileEntry : folder.listFiles()) {
                if (fileEntry.isFile() && fileEntry.getName().endsWith(".input")) {
                    try (InputStream input = new FileInputStream(folder.getPath().replace('\\', '/') + "/" + fileEntry.getName())) {
                        byte[] byt = input.readAllBytes();
                        plainText = new String(byt, "CP1146");
                        message = context.createTextMessage();
                        message.setText(plainText);
                    } catch (Exception e) {
                        System.out.println("Exception occurred");
                    }
                }
            }
            System.out.println("Message is ready!");

            // NFT Specifics
            long noOfMessages = Long.parseLong(prop.getProperty("nft.flood.count"));

            Instant firstMessageTime;
            Instant lastMessageTime;
            double actualTps = 0;
            if (noOfMessages > 0) {
                long i = 0;
                if (noOfMessages > 1) {
                    // First Message Push
                    producer.send(destination, message);
                    timestamp.setTime(message.getJMSTimestamp());
                    firstMessageTime = timestamp.toInstant();
                    logger.info("Message (" + (i + 1) + ") pushed at" + timestamp.toInstant().toString() + "with JMS Message ID" + message.getJMSMessageID() + "after start duration" + Duration.between(firstMessageTime, Instant.now()));

                    i = i + 1;

                    while (i < noOfMessages - 1) {
                        producer.send(destination, message);
                        timestamp.setTime(message.getJMSTimestamp());
                        logger.info("Message (" + (i + 1) + ") pushed at" + timestamp.toInstant().toString() + "with JMS Message ID" + message.getJMSMessageID() + "after start duration" + Duration.between(firstMessageTime, Instant.now()));
                        i++;
                    }

                    // Last Message Push
                    producer.send(destination, message);
                    timestamp.setTime(message.getJMSTimestamp());
                    logger.info("Message (" + (i + 1) + ") pushed at" + timestamp.toInstant().toString() + "with JMS Message ID" + message.getJMSMessageID() + "after start duration" + Duration.between(firstMessageTime, Instant.now()));
                    lastMessageTime = timestamp.toInstant();

                    Duration duration = Duration.between(firstMessageTime, lastMessageTime);
                    double totalNanos = (duration.getSeconds() * 1000_000_000) + duration.getNano();
                    double totalSeconds = totalNanos / 1000_000_000;
                    try {
                        actualTps = noOfMessages / totalSeconds;
                    } catch (ArithmeticException e) {
                        logger.severe("Arithmetic exception occurred as the transactions ended in less than a second...");
                    }
                } else {
                    producer.send(destination, message);
                    timestamp.setTime(message.getJMSTimestamp());
                    firstMessageTime = timestamp.toInstant();
                    logger.info("Message (" + (i + 1) + ") pushed at" + timestamp.toInstant().toString() + "with JMS Message ID" + message.getJMSMessageID() + "after start duration" + Duration.between(firstMessageTime, Instant.now()));
                    lastMessageTime = timestamp.toInstant();
                    logger.info("Since only one message is delivered, the TPS cannot be calculated and hence marked as 0.");
                }

                String summary = "\nNFT Message Summary:" + "\nTotal Messages: " + noOfMessages + "\nFirst Message reached MQ at: " + firstMessageTime + "\nlast Message reached MQ at: " + lastMessageTime.toString() + "\nTPS Rate: " + actualTps;
                logger.severe(summary);
            } else {
                logger.severe("Cannot push messages less than zero count..." + "\nPlease enter a valid parameter values...");
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
     *
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