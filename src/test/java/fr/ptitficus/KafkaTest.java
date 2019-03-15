package fr.ptitficus;

import akka.Done;
import akka.actor.ActorSystem;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.kafka.testkit.javadsl.EmbeddedKafkaJunit4Test;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.AfterClass;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;

public class KafkaTest extends EmbeddedKafkaJunit4Test {

    private static final ActorSystem sys = ActorSystem.create("KafkaTest");
    private static final Materializer mat = ActorMaterializer.create(sys);
    private final Executor executor = Executors.newSingleThreadExecutor();


    public KafkaTest() {
        super(sys, mat, 9090);
    }

    @Test
    public void foo() throws Exception {
        String topic = createTopic(1, 1, 1);
        // #plainSink
        CompletionStage<Done> done =
                Source.range(1, 100)
                        .map(number -> number.toString())
                        .map(value -> new ProducerRecord<String, String>(topic, value))
                        .runWith(Producer.plainSink(producerDefaults()), materializer);
        // #plainSink

        Consumer.DrainingControl<List<ConsumerRecord<String, String>>> control =
                consumeString(topic, 100);
        assertEquals(Done.done(), resultOf(done));
        assertEquals(Done.done(), resultOf(control.isShutdown()));
        CompletionStage<List<ConsumerRecord<String, String>>> result =
                control.drainAndShutdown(executor);
        assertEquals(100, resultOf(result).size());
    }

    @AfterClass
    public static void afterClass() {
        TestKit.shutdownActorSystem(sys);
    }
}

