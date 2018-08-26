package com.cwoongc.study.rabbitmq_fundmental.helloworld;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Send {

    private final static String QUEUE_NAME = "hello";

    public static void main(String[] args) throws IOException, TimeoutException {

        /**
         * ConnectionFactory create
         */
        ConnectionFactory cf = new ConnectionFactory();
        cf.setHost("localhost");
        cf.setPort(5672);
        cf.setUsername("rabbitmq");
        cf.setPassword("rabbitmq");

        /**
         * Connection create
         * TCP 연결 획득. 큰 자원소모
         */
        Connection connection = cf.newConnection();

        /**
         * Channel create
         * 발행자, 소비자, 브로커 사이의 논리적 연결.
         * 하나의 Connection에 다수의 Channel 설정 가능.
         * 용도 : 특정 Client와 Broker간에 간섭이 일어나지 않도록 상호작용을 분리시켜줌.
         * 특장점 : 연결에 비용이 많이드는 개별 TCP Connection을 열지 않고서 사용가능.
         * 닫힘 : 프로토콜 예외 발생시 닫힐 있음
         */
        Channel channel = connection.createChannel();

        /**
         * 모든 amqp operation은 Channel을 통해 수행
         */

        /**
         * Channel : QUEUE Declare
         * To send, we must declare a queue for us to send to; then we can publish a message to the queue:
         * Queue를 선언해야 메세지를 브로커에 보낼수있음.

         * Declaring a queue is idempotent - it will only be created if it doesn't exist already.
         * Queue 선언은 멱등적 작업으로 한번 선언하면 다시 선언해도 첫번 선언시 생성된게 활용됨.
         * Queue 선언시 큐이름 등의 큐의 속성을 정해줌
         *
         */
        channel.queueDeclare(QUEUE_NAME,false,false,false,null);


        /**
         * Channel : message Publish
         * The message content is a byte array, so you can encode whatever you like there.
         *
         * 메세지 발행은 exchange를 지정하여 해당 익스체인지의 라우팅 규칙사용하게 한다.
         * 존재하지 않는 exchange를 넘기면 channel레벨의 프로토콜 예외가 발생한다.
         *
         * Publish a message. Publishing to a non-existent exchange will result in a channel-level protocol exception, which closes the channel. Invocations of Channel#basicPublish will eventually block if a resource-driven alarm  is in effect.
         *
         * Params:
         * exchange – the exchange to publish the message to
         * routingKey – the routing key
         * props – other properties for the message - routing headers etc
         * body – the message body
         *
         */
        String message = "Helllo World!!!";
        channel.basicPublish("", QUEUE_NAME, null, message.getBytes() );



        System.out.println(" [x] Sent '" + message + "'");






    }
}
