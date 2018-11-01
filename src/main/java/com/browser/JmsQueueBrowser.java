package com.browser;

import org.apache.commons.cli.*;

import java.net.URISyntaxException;
import java.util.Enumeration;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

public class JmsQueueBrowser {
  public static void main(String[] args) throws URISyntaxException, Exception {

    Options options = new Options();

    Option broker_url_option = new Option("b", "broker_url", true, "Fully Qualified Broker Url (defaults to tcp://localhost:61616)");
    broker_url_option.setRequired(false);
    options.addOption(broker_url_option);

    Option queue_name_option = new Option("q", "queue_name", true, "Queue Name to Browse (defaults to TEST.Q)");
    queue_name_option.setRequired(false);
    options.addOption(queue_name_option);

    Option username_option = new Option("u", "username", true, "Username to connect to the broker (defaults to admin)");
    username_option.setRequired(false);
    options.addOption(username_option);

    Option password_option = new Option("p", "password", true, "Password to connect to the broker (defaults to admin)");
    password_option.setRequired(false);
    options.addOption(password_option);

    Option producer_option = new Option("c", "create", true, "Should a message be created on the given queue before browsing (defaults to false)");
    producer_option.setRequired(false);
    options.addOption(producer_option);

    Option selector_option = new Option("s", "selector", true, "Selector for searching the queue in the form \"size = 'big'\" (defaults to NULL)");
    selector_option.setRequired(false);
    options.addOption(selector_option);

    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd = null;

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("java -jar JmsQueueBrowser.jar", options);

      System.exit(1);
    }

    String broker_url = cmd.getOptionValue("broker_url") == null ? "tcp://localhost:61616" : cmd.getOptionValue("broker_url"); 
    String queue_name = cmd.getOptionValue("queue_name") == null ? "TEST.Q" : cmd.getOptionValue("queue_name"); 
    String username = cmd.getOptionValue("username") == null ? "admin" : cmd.getOptionValue("username"); 
    String password = cmd.getOptionValue("password") == null ? "admin" : cmd.getOptionValue("password"); 
    Boolean create = cmd.getOptionValue("create") == null ? false : Boolean.valueOf(cmd.getOptionValue("create")); 

    String selector = cmd.getOptionValue("selector");

    Connection connection = null;

    try {
      ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
          username, password, broker_url);
      connection = connectionFactory.createConnection();
      connection.start();
      Session session = connection.createSession(false,
          Session.AUTO_ACKNOWLEDGE);
      Queue queue = session.createQueue(queue_name);

      if (create) {
        // Producer
        MessageProducer producer = session.createProducer(queue);
        Message msg1 = session.createTextMessage("Big");
        Message msg2 = session.createTextMessage("Small");
        msg1.setStringProperty("size", "big");
        msg2.setStringProperty("size", "small");
        System.out.println("Sending 'Big' message");
        producer.send(msg1);
        System.out.println("Sending 'Small' message");
        producer.send(msg2);
      }

      MessageConsumer consumer = session.createConsumer(queue);

      System.out.println("Browsing through the elements in queue: " + queue_name);
    System.out.println("With selector: "+selector);
      QueueBrowser browser = session.createBrowser(queue, selector);
      Enumeration e = browser.getEnumeration();
      while (e.hasMoreElements()) {
        TextMessage message = (TextMessage) e.nextElement();
        System.out.println("Browse [" + message.getText() + "]");
      }
      System.out.println("Done");
      browser.close();

      session.close();
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

}

