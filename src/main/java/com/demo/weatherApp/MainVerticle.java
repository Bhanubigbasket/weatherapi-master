package com.demo.weatherApp;

import io.vertx.core.*;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.micrometer.*;

public class MainVerticle extends AbstractVerticle {

  private  static  final Logger logger = LoggerFactory.getLogger(SensorVerticle.class);
  public  static  void  main(String[] args)
  {
    Vertx vertex = Vertx.vertx(new VertxOptions().setMetricsOptions(
      new MicrometerMetricsOptions()
        .setInfluxDbOptions(new VertxInfluxDbOptions().setEnabled(true)
          .setUri("http://localhost:8086")
          .setDb("BigBasket")
          .setUserName("Bhanu")
          .setPassword("Bhanu@1234"))
        .setEnabled(true)
    ));

    vertex.deployVerticle(new SensorVerticle());
    vertex.eventBus().consumer("temperature",message ->
    {
      logger.info(message.body());
    });
    vertex.eventBus().publish("temperature", "test message");
  }
}
