package com.cwoongc.study.rabbitmq_fundmental.work_queues;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * it needs to fake a second of work for every dot in the message body. It will handle delivered messages and perform the task, so let's call it Worker.java
 */
public class Worker {

    public static final String TASK_QUEUE_NAME = "task_queue";


    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);
        factory.setUsername("rabbitmq");
        factory.setPassword("rabbitmq");

        Connection connection = factory.newConnection();

        Channel channel = connection.createChannel();

        /**
         * Message durability
         * We have learned how to make sure that even if the consumer dies, the task isn't lost. But our tasks will still be lost if RabbitMQ server stops.
         *
         * When RabbitMQ quits or crashes it will forget the queues and messages unless you tell it not to.
         * Two things are required to make sure that messages aren't lost: we need to mark both the queue and messages as durable.
         *
         * First, we need to make sure that RabbitMQ will never lose our queue. In order to do so, we need to declare it as durable:
         *
         * At this point we're sure that the task_queue queue won't be lost even if RabbitMQ restarts.
         * Now we need to mark our messages as persistent - by setting MessageProperties (which implements BasicProperties) to the value PERSISTENT_TEXT_PLAIN.
         */
        boolean durable = true;

        channel.queueDeclare(TASK_QUEUE_NAME,durable,false,false,null);

        /**
         * Request a specific prefetchCount "quality of service" settings for this channel.
         *
         * Params:
         * prefetchCount – maximum number of messages that the server will deliver, 0 if unlimited
         */
        /**
         * Fair dispatch
         *
         * You might have noticed that the dispatching still doesn't work exactly as we want.
         * For example in a situation with two workers, when all odd messages are heavy and even messages are light,
         * one worker will be constantly busy and the other one will do hardly any work.
         * Well, RabbitMQ doesn't know anything about that and will still dispatch messages evenly.
         *
         * This happens because RabbitMQ just dispatches a message when the message enters the queue.
         * It doesn't look at the number of unacknowledged messages for a consumer.
         * It just blindly dispatches every n-th message to the n-th consumer.
         *
         * In order to defeat that we can use the basicQos method with the prefetchCount = 1 setting.
         * This tells RabbitMQ not to give more than one message to a worker at a time.
         * Or, in other words, don't dispatch a new message to a worker until it has processed and acknowledged the previous one.
         * Instead, it will dispatch it to the next worker that is not still busy.
         *
         * Note about queue size
         * If all the workers are busy, your queue can fill up.
         * You will want to keep an eye on that, and maybe add more workers, or have some other strategy.
         */
        int prefetchCount = 1;
        channel.basicQos(prefetchCount); // accept only one unack-ed message at a time


        /**
         * Consumer create (소비자 로직 생성. 소비자 로직은 채널을 통해 "소비" 메소드로 등록되고, 비동기로 콜백된다.)
         * : consume callback logic
         */
        final Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {

                //body가 callback된 메세지
                String message = new String(body, "UTF-8");

                System.out.println(" [x] Received '" + message + "'");
                try {
                    //소비시에 메세지 가지고 작업을 처리한다.
                    doWork(message);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    System.out.println(" [x] Done");
                    channel.basicAck(envelope.getDeliveryTag(), false);
                }
            }
        };

        /**
         * message acknowledgments
         * 소비자가 message를 comsume하고 나서 broker에게 안전하게 메세지를 소비했음을 알리는 것.
         * broker는 본 ack를 받은후에야 해당 message를 삭제한다.
         *
         * 만약 ack없이 소비자가 죽게되면, 브로커는 message가 완전히 처리되지 못했음으로 인지하고 message를 다시 큐에 넣는다.
         *
         * message timeout은 없으며, 오직 소비자가 죽었을때만 브로커는 message를 다른 소비자에게 다시 배달한다.
         *
         * 본 동작방식은 소비자가 작업중에 작업을 완료하지 못하고 죽게 될경우
         * message 가 유실되므로 이것을 방지하기 위한
         * rabbitmq의 동작방식이다.
         *
         * RabbitMQ supports message acknowledgments.
         * An ack(nowledgement) is sent back by the consumer to tell RabbitMQ that a particular message has been received,
         * processed and that RabbitMQ is free to delete it.
         *
         * If a consumer dies (its channel is closed, connection is closed, or TCP connection is lost) without sending an ack,
         * RabbitMQ will understand that a message wasn't processed fully and will re-queue it.
         * If there are other consumers online at the same time,
         * it will then quickly redeliver it to another consumer.
         * That way you can be sure that no message is lost, even if the workers occasionally die.
         *
         * There aren't any message timeouts;
         * RabbitMQ will redeliver the message when the consumer dies.
         * It's fine even if processing a message takes a very, very long time.
         *
         * autoAck가 true이면 이는 브로커가 메세지만 소비자에게 전달했으면 ack를 받은걸로 치겠다는 의미로
         * 상기 message acknowledgements 기능을 disable 시킨다.
         *
         * aoutAck가 false여야 message ack 기능이 정상동작하고, 이는 default 설정이다.
         */
        boolean autoAck = false; // true 메세지를 받은걸로 ack를 보낸걸로 치기에 메세지 유실의 위험이 존재한다. default는 false.
        channel.basicConsume(TASK_QUEUE_NAME, autoAck, consumer);

    }

    /**
     * time-consuming task를 시뮬레이션 하기 위해 만든 fake work.
     * message속에 '.' 갯 수 만큼 스레드를 1초 슬립시킨다.
     * 메세지에 '.' 갯수를 조절하여 작업완료시간을 조정하기 위함.
     *
     * @param task
     * @throws InterruptedException
     */
    private static void doWork(String task) throws InterruptedException {


        for (char ch: task.toCharArray()) {
            if (ch == '.') Thread.sleep(1000);
        }
    }



}
