package kafka;

import maprstreams.config.ConfigConstants;
import kafka.consume.KafkaEvent;
import kafka.consume.SimpleConsumer;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Basic test show how to consume messages from kafka and then publish them on the event bus.
 *

 */
@RunWith(VertxUnitRunner.class)
public class SimpleConsumerTest {
    private static final Logger logger = LoggerFactory.getLogger(SimpleConsumerTest.class);

    private static Vertx vertx;

    @Ignore("This is an integration test comment out to actually run it")
    @Test
    public void testMessageReceipt(TestContext testContext) {
        Async async = testContext.async();

        vertx = Vertx.vertx();

        JsonObject consumerConfig = new JsonObject();
        consumerConfig.put(ConfigConstants.GROUP_ID, "testGroup");
        consumerConfig.put(ConfigConstants.ZK_CONNECT, "localhost:2181");
        consumerConfig.put(ConfigConstants.BOOTSTRAP_SERVERS, "localhost:9092");
        List<String> topics = new ArrayList<>();
        topics.add("test");
        consumerConfig.put("topics", new JsonArray(topics));

        vertx.deployVerticle(SimpleConsumer.class.getName(),
            new DeploymentOptions().setConfig(consumerConfig), deploy -> {
                if (deploy.failed()) {
                    logger.error("", deploy.cause());
                    testContext.fail("Could not deploy verticle");
                    async.complete();
                    vertx.close();
                } else {
                    // have the test run for 20 seconds to give you enough time to get a message off
                    long timerId = vertx.setTimer(20000, theTimerId ->
                    {
                        logger.info("Failed to get any messages");
                        testContext.fail("Test did not complete in 20 seconds");
                        async.complete();
                        vertx.close();
                    });

                    logger.info("Registering listener on event bus for kafka messages");

                    vertx.eventBus().consumer(SimpleConsumer.EVENTBUS_DEFAULT_ADDRESS, (Message<JsonObject> message) -> {
                        assertTrue(message.body().toString().length() > 0);
                        logger.info("got message: " + message.body());
                        KafkaEvent event = new KafkaEvent(message.body());
                        vertx.cancelTimer(timerId);
                        async.complete();
                        vertx.close();
                    });
                }
            }
        );
    }
}
