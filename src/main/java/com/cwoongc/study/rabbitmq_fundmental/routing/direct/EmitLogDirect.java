package com.cwoongc.study.rabbitmq_fundmental.routing.direct;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class EmitLogDirect {
    public static final String EXCHANGE_NAME = "direct_logs";
    public static final String EXCHANGE_TYPE = "direct";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("rabbitmq");
        factory.setPassword("rabbitmq");


        Connection connection = factory.newConnection();

        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE);

        String severity = getSeverity(args); //will be routingKey
        String message = getMessage(args);

        channel.basicPublish(EXCHANGE_NAME, severity, null, message.getBytes());


        channel.close();
        connection.close();


    }

    private static String getMessage(String[] args) {

        if(args[1] == null ) {
            return "Hello World!!";
        } else {
            return args[1];
        }


    }

    private static String getSeverity(String[] args) {

        String severity = null;

        if(args[0] == null)  {
            severity = "error";
        } else {
            severity = args[0];
        }

        return severity;
    }


}
