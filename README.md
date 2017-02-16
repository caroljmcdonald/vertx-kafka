

# Vert.x Kafka

The Vert.x kafka library allows asynchronous publishing and receiving of messages on MapR Streams topic using the Kafka API through the vert.x event bus.

####You can use this with MapR Streams on the MapR Sandbox

This is a multi-threaded worker library that consumes kafka messages and then re-broadcast them on an address on the vert.x event bus.

## Getting Started




## Consumer

Listening for messages coming from a kafka broker.

### Configuration

```json
{

    "group.id" : "<kafkaConsumerGroupId>",
    "backoff.increment.ms" : "<backTimeInMilli>",
    "autooffset.reset" : "<kafkaAutoOffset>",
    "topics" : ["<topic1>", "<topic2>"],
    "eventbus.address" : "<default kafka.message.consumer>",
    "consumer.poll.interval.ms" : <default 100 ms>
}
```

For example:

```json
{

    "group.id" : "testGroup",
    "autooffset.reset" : "earliest",
    "topics" : ["/user/user01/stream:ekg"],
    "eventbus.address" : "kafka.to.vertx.bridge",
    "consumer.poll.interval.ms" : 1000
}
```
Field breakdown:


* `group.id` the kafka consumer group name that will be consuming related to
* `autooffset.reset` how to reset the offset
* `topics` the kafka topics to listen for
* `eventbus.address` the vert.x address to publish messages onto when received form kafka
* `consumer.poll.interval.ms` how often to try and consume messages

For a deeper look at kafka configuration parameters check [this](http://kafka.apache.org/documentation.html) page out.

### Usage

You should only need one consumer per application.

#### Deploy the verticle in your server

```java

vertx = Vertx.vertx();

// sample config
JsonObject consumerConfig = new JsonObject();
consumerConfig.put(ConfigConstants.GROUP_ID, "testGroup");
List<String> topics = new ArrayList<>();
topics.add("testTopic");
consumerConfig.put("topics", new JsonArray(topics));

deployKafka(config);

public void deployKafka(JsonObject config) {
   // use your vert.x reference to deploy the consumer verticle
	 vertx.deployVerticle(MessageConsumer.class.getName(),
      new DeploymentOptions().setConfig(config),
			deploy -> {
   		     if(deploy.failed()) {
        	     System.err.println(String.format("Failed to start kafka consumer verticle, ex: %s", deploy.cause()));
               vertx.close()
               return;
            }
            System.out.println("kafka consumer verticle started");
      }
	);
}
```
#### Listen for messages

```java

vertx.eventBus().consumer(MessageConsumer.EVENTBUS_DEFAULT_ADDRESS,
	message -> {
		   System.out.println(String.format("got message: %s", message.body()))
		   // message handling code
		   KafkaEvent event = new KafkaEvent(message.body());
	 });
```

#### Consumer Errors

You can listen on the address `kafka.producer.error` for errors from the kafka producer.

## Producer

Send a message to a kafka cluster on a predefined topic.

### Configuration

```json
{
    "serializer.class":"<the default encoder>",
    "key.serializer":"<the key encoder>",
    "value.serializer":"<the value encoder>",
    "default_topic":"<default kafka topic to send to>,"
    "eventbus.address":"<the event bus topic where you send messages to send to kafka>"
    "max.block.ms" : <defaults to 60000>
}
```

For example:

```json
{
    "serializer.class":"org.apache.kafka.common.serialization.StringSerializer",
    "default_topic":"/user/user01/stream:ekg"
}
```

* `serializer.class` The serializer class for messages
* `key.serializer` The serializer class for keys, defaults to the serializel.class if not set
* `value.serializer` The serializer class for values, defaults to the serializel.class if not set

* `default_topic` The default topic in kafka to send to
* `eventbus.address` The address to listen to on the event bus, defaults to 'kafka.message.publisher'
* `max.block.ms` How long should the sender wait before getting meta data or time out in ms.

For a deeper look at kafka configuration parameters check [this](https://kafka.apache.org/08/configuration.html) page out.

### Usage

You should only need one producer per application.

#### Deploy the verticle in your server

```java

vertx = Vertx.vertx();

// sample config
JsonObject producerConfig = new JsonObject();
producerConfig.put("bootstrap.servers", "localhost:9092");
producerConfig.put("serializer.class", "org.apache.kafka.common.serialization.StringSerializer");
producerConfig.put("default_topic", "testTopic");

deployKafka(producerConfig);

public void deployKafka(JsonObject config) {
   // use your vert.x reference to deploy the consumer verticle
	 vertx.deployVerticle(MessageProducer.class.getName(),
     new DeploymentOptions().setConfig(config),
		 deploy -> {
   	   if(deploy.failed()) {
         System.err.println(String.format("Failed to start kafka producer verticle, ex: %s", deploy.cause()));
          vertx.close()
          return;
       }
       System.out.println("kafka producer verticle started");
   });
}
```
#### Send message to kafka topic

```java

KafkaPublisher publisher = new KafkaPublisher(vertx.eventBus());

// send to the default topic
publisher.send("a test message on a default topic");
// send to a specific topic
publisher.send("SomeSpecialTopic", "a test message on a default topic");
// send to a specific topic with custom key
publisher.send("SomeSpecialTopic", "aUserId", "a test message on a default topic");
// send to a specific topic and partition
publisher.send("SomeSpecialTopic", "", 5, "a test message on a default topic");

```
#### Producer Errors

You can listen on the address `kafka.producer.error` for errors from the kafka producer.

