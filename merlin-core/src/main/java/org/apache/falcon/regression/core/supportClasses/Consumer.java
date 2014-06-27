/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.falcon.regression.core.supportClasses;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Logger;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;

public class Consumer extends Thread {
    /*URL of the JMS server
    url = "tcp://host:61616?daemon=true";
    ActiveMQConnection.DEFAULT_BROKER_URL;
    Name of the queue we will receive messages from
    String subject = "IVORY.TOPIC";*/

    String url;
    String subject;
    Logger logger = Logger.getLogger(this.getClass());

    List<HashMap<String, String>> messageData;

    public List<HashMap<String, String>> getMessageData() {
        return messageData;
    }

    public Consumer(String subject, String url) {
        super(subject);
        this.subject = subject;
        this.url = url;
        messageData = new ArrayList<HashMap<String, String>>();
    }

    public void run() {
        try {
            readMessage();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void readMessage() throws JMSException {
        // Getting JMS connection from the server
        ConnectionFactory connectionFactory
            = new ActiveMQConnectionFactory(url);
        Connection connection = connectionFactory.createConnection();
        connection.start();

        // Creating session for sending messages
        Session session = connection.createSession(false,
            Session.AUTO_ACKNOWLEDGE);

        // Getting the queue 'TESTQUEUE'
        Destination destination = session.createTopic(subject);

        // MessageConsumer is used for receiving (consuming) messages
        MessageConsumer consumer = session.createConsumer(destination);

        /*Here we receive the message.
        By default this call is blocking, which means it will wait
        for a message to arrive on the queue.*/
        try {
            logger.info("Starting to receive messages.");
            while (true) {
                Message m = consumer.receive();
                logger.info(m);
                if (m != null) {
                    MapMessage message = (MapMessage) m;
                    Enumeration mapNames = message.getMapNames();
                    HashMap<String, String> temp = new HashMap<String, String>();
                    while (mapNames.hasMoreElements()) {
                        String objectName = mapNames.nextElement().toString();
                        temp.put(objectName, message.getString(objectName));
                    }
                    messageData.add(temp);
                }
            }
        } finally {
            logger.info("Stopping to receive messages.");
            connection.close();
        }
    }
}
