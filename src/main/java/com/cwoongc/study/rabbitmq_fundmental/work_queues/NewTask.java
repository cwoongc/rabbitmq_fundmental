package com.cwoongc.study.rabbitmq_fundmental.work_queues;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class NewTask {

    public static final String TASK_QUEUE_NAME = "task_queue";

    public static void main(String[] args) throws IOException, TimeoutException {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);
        factory.setUsername("rabbitmq");
        factory.setPassword("rabbitmq");

        Connection connection = factory.newConnection();

        Channel channel = connection.createChannel();

        boolean durable = true;
        channel.queueDeclare(TASK_QUEUE_NAME,durable,false,false,null);


        /**
         * CLI string 아규먼트를 char 사이사이 공백을 채워서 메세지로 삼는다.
         */
        String message = getMessage(args);

        /**
         * Channel : message Publish
         *
         * MessageProperties.PERSISTENT_TEXT_PLAIN :
         * Note on message persistence
         * Marking messages as persistent doesn't fully guarantee that a message won't be lost.
         * Although it tells RabbitMQ to save the message to disk,
         * there is still a short time window when RabbitMQ has accepted a message and hasn't saved it yet.
         * Also, RabbitMQ doesn't do fsync(2) for every message
         *  -- it may be just saved to cache and not really written to the disk.
         *     The persistence guarantees aren't strong, but it's more than enough for our simple task queue.
         *     If you need a stronger guarantee then you can use "publisher confirms".
         */
        channel.basicPublish("", TASK_QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
        System.out.println(" [x] Sent '" + message + "'");

    }


    private static String getMessage(String[] strings){
        if (strings.length < 1)
            return "Hello World!";
        return joinStrings(strings, " ");
    }


    private static String joinStrings(String[] strings, String delimiter) {
        int length = strings.length;
        if (length == 0) return "";
        StringBuilder words = new StringBuilder(strings[0]);
        for (int i = 1; i < length; i++) {
            words.append(delimiter).append(strings[i]);
        }
        return words.toString();
    }
}
