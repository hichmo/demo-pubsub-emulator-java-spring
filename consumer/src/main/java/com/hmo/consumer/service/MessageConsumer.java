package com.hmo.consumer.service;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import io.grpc.ManagedChannelBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Configuration
public class MessageConsumer {

    private TransportChannelProvider channelProvider;

    private static final String PROJECT = "hmo-project";
    private static final String SUBSCRIPTION_NAME = "SUBSCRIBER";
    private static final String TOPIC_NAME = "test-topic-id";

    private static final String hostPort = "127.0.0.1:8085";

    private ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of("hmo", SUBSCRIPTION_NAME);


    @Bean
    public void consume() throws IOException {

        CredentialsProvider credentialsProvider = NoCredentialsProvider.create();

        channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel
                .create(ManagedChannelBuilder.forTarget(hostPort).usePlaintext().build()));

        /*
        SubscriptionAdminSettings subscriptionAdminSettings = SubscriptionAdminSettings.newBuilder()
                .setCredentialsProvider(credentialsProvider)
                .setTransportChannelProvider(channelProvider)
                .build();

        SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(subscriptionAdminSettings);
            TopicName topic = TopicName.ofProjectTopicName(PROJECT, TOPIC_NAME);
            PushConfig pushConfig = PushConfig.newBuilder().build();
            int ackDeadlineSeconds = 2135351438;
            Subscription subscription =
                    subscriptionAdminClient.createSubscription(subscriptionName, topic, pushConfig, ackDeadlineSeconds);
         */

        ProjectSubscriptionName subscriptionName =
                ProjectSubscriptionName.of("hmo-project", "SUBSCRIBER");

        MessageReceiver receiver =
                (PubsubMessage message, AckReplyConsumer consumer) -> {
                    System.out.println("Id: " + message.getMessageId());
                    System.out.println("Data: " + message.getData().toStringUtf8());
                    consumer.ack();
                };

        Subscriber subscriber = null;
        try {
            subscriber = Subscriber.newBuilder(subscriptionName, receiver)
                    .setCredentialsProvider(credentialsProvider)
                    .setChannelProvider(channelProvider)

                    .build();
            subscriber.startAsync().awaitRunning();
            System.out.printf("Listening for messages on %s:\n", subscriptionName.toString());
            subscriber.awaitTerminated(30, TimeUnit.SECONDS);
        } catch (TimeoutException timeoutException) {
            subscriber.stopAsync();
        }

    }

}
