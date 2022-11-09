package com.hmo.pubsub.producer.controller;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.*;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.*;
import com.hmo.pubsub.producer.domain.model.Message;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping(path = "/messages")
public class MessageController {

    private static final String PROJECT = "hmo-project";
    private static final String SUBSCRIPTION_NAME = "SUBSCRIBER";
    private static final String TOPIC_NAME = "test-topic-id";

    private static final String hostPort = "127.0.0.1:8085";

    private ManagedChannel channel;
    private TransportChannelProvider channelProvider;
    private TopicAdminClient topicAdmin;

    private Publisher publisher;
    private SubscriberStub subscriberStub;
    private SubscriptionAdminClient subscriptionAdminClient;

    private ProjectTopicName topicName = ProjectTopicName.of(PROJECT, TOPIC_NAME);
    private ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(PROJECT, SUBSCRIPTION_NAME);
    private Subscription subscription;

    @PostMapping
    void postMessage(@RequestBody Message message) throws IOException, InterruptedException, ExecutionException {

        channel = ManagedChannelBuilder.forTarget(hostPort).usePlaintext().build();
        channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));

        CredentialsProvider credentialsProvider = NoCredentialsProvider.create();

        topicAdmin = createTopicAdmin(credentialsProvider);
        topicAdmin.createTopic(topicName);

        publisher = createPublisher(credentialsProvider);
        subscriberStub = createSubscriberStub(credentialsProvider);
        subscriptionAdminClient = createSubscriptionAdmin(credentialsProvider);
        subscription = subscriptionAdminClient.createSubscription(subscriptionName, topicName, PushConfig.getDefaultInstance(), 0);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                .setData(ByteString.copyFrom(message.getContent(), Charset.defaultCharset()))
                .build();
        publisher.publish(pubsubMessage).get();

        PullRequest pullRequest = PullRequest.newBuilder()
                .setMaxMessages(1)
                .setReturnImmediately(true) // return immediately if messages are not available
                .setSubscription(subscription.getName())
                .build();

        PullResponse pullResponse = subscriberStub.pullCallable().call(pullRequest);
        String receiveMessageText = pullResponse.getReceivedMessages(0).getMessage().getData().toStringUtf8();

        System.out.println("pub sub " + receiveMessageText);
    }

    @DeleteMapping
    void delete() throws IOException {

        channel = ManagedChannelBuilder.forTarget(hostPort).usePlaintext().build();
        channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
        CredentialsProvider credentialsProvider = NoCredentialsProvider.create();
        subscriptionAdminClient = createSubscriptionAdmin(credentialsProvider);
        subscriptionAdminClient.deleteSubscription(SUBSCRIPTION_NAME);
        channel.shutdownNow();
    }
    private TopicAdminClient createTopicAdmin(CredentialsProvider credentialsProvider) throws IOException {
        return TopicAdminClient.create(
                TopicAdminSettings.newBuilder()
                        .setTransportChannelProvider(channelProvider)
                        .setCredentialsProvider(credentialsProvider)
                        .build()
        );
    }

    private SubscriptionAdminClient createSubscriptionAdmin(CredentialsProvider credentialsProvider) throws IOException {
        SubscriptionAdminSettings subscriptionAdminSettings = SubscriptionAdminSettings.newBuilder()
                .setCredentialsProvider(credentialsProvider)
                .setTransportChannelProvider(channelProvider)
                .build();
        return SubscriptionAdminClient.create(subscriptionAdminSettings);
    }

    private Publisher createPublisher(CredentialsProvider credentialsProvider) throws IOException {
        return Publisher.newBuilder(topicName)
                .setChannelProvider(channelProvider)
                .setCredentialsProvider(credentialsProvider)
                .build();
    }

    private SubscriberStub createSubscriberStub(CredentialsProvider credentialsProvider) throws IOException {
        SubscriberStubSettings subscriberStubSettings = SubscriberStubSettings.newBuilder()
                .setTransportChannelProvider(channelProvider)
                .setCredentialsProvider(credentialsProvider)
                .build();
        return GrpcSubscriberStub.create(subscriberStubSettings);
    }

}
