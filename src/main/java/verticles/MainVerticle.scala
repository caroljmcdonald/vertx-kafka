package eu.fakod.streamprocessing.verticles

import org.vertx.scala.platform.Verticle


class MainVerticle extends Verticle {

  override def start() = {
    val appConfig = container.config()

    val kafkaVerticleConfig = appConfig.getObject("KafkaVerticle")
    container.deployVerticle("verticles.KafkaVerticle", kafkaVerticleConfig)


  }
}
