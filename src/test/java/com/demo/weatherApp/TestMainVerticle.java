package com.demo.weatherApp;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.sqlclient.PoolOptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;
//Test cases to call APIs and validate the response,
// We can extend it to check DB as well to check input to test case with DB entries.
// It can be extended to negative Scenario as well
@ExtendWith(VertxExtension.class)
public class TestMainVerticle {

  @BeforeEach
  void deploy_verticle(Vertx vertx, VertxTestContext testContext) {
    vertx.deployVerticle(new SensorVerticle(), testContext.succeeding(id -> testContext.completeNow()));
    refershDB(vertx);
  }

  @BeforeAll
  public static void init(Vertx vertx)
  {
    refershDB(vertx);
  }

  @Test
  void verticle_deployed(Vertx vertx, VertxTestContext testContext) throws Throwable {
    testContext.completeNow();
  }

  @Test
  public void testGetAPIWithPlace(Vertx vertx, VertxTestContext testContext) {
    HttpClient client = vertx.createHttpClient();

    client.request(HttpMethod.GET, 7080, "localhost", "/api/v1/getdata/place/surat")
      .compose(req -> req.send().compose(HttpClientResponse::body))
      .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
        assertThat(buffer.toString()).contains("\"name\" : \"Surat\"").contains("\"lon\" : 72.8333").contains("\"lat\" : 21.1667");
        testContext.completeNow();
      })));
  }

  @Test
  public void testGetAPIWithLatLon(Vertx vertx, VertxTestContext testContext) {
    HttpClient client = vertx.createHttpClient();

    client.request(HttpMethod.GET, 7080, "localhost", "/api/v1/getdata/latlon/77.7036/12.8029")
      .compose(req -> req.send().compose(HttpClientResponse::body))
      .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
        assertThat(buffer.toString()).contains("\"name\" : \"Nybyen\"");
        testContext.completeNow();
      })));
  }

  @Test
  public void testGetAPIWithZipCode(Vertx vertx, VertxTestContext testContext) {
    HttpClient client = vertx.createHttpClient();

    client.request(HttpMethod.GET, 7080, "localhost", "/api/v1/getdata/zip/IN/560099")
      .compose(req -> req.send().compose(HttpClientResponse::body))
      .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
        assertThat(buffer.toString()).contains("\"name\" : \"Chandapura\"");
        testContext.completeNow();
      })));
  }

  @Test
  public void testGetAPIWithProvideRange(Vertx vertx, VertxTestContext testContext) {
    HttpClient client = vertx.createHttpClient();
    client.request(HttpMethod.GET, 7080, "localhost", "/api/v1/savedata/place/Bengaluru")
      .compose(req -> req.send().compose(HttpClientResponse::body))
      .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
        assertThat(buffer.toString()).contains("Got Entry for Bengaluru");

        client.request(HttpMethod.GET, 7080, "localhost", "/api/v1/savedata/place/Mysore")
          .compose(req -> req.send().compose(HttpClientResponse::body))
          .onComplete(testContext.succeeding(buffer1 -> testContext.verify(() -> {
            assertThat(buffer1.toString()).contains("Got Entry for Mysore");

            client.request(HttpMethod.GET, 7080, "localhost", "/api/v1/savedata/place/Chandapura")
              .compose(req -> req.send().compose(HttpClientResponse::body))
              .onComplete(testContext.succeeding(buffer2 -> testContext.verify(() -> {
                assertThat(buffer2.toString()).contains("Got Entry for Chandapura");

                client.request(HttpMethod.GET, 7080, "localhost", "/api/v1/search/latlon/range/12.9762/77.6033/500")
                  .compose(req -> req.send().compose(HttpClientResponse::body))
                  .onComplete(testContext.succeeding(buffer3 -> testContext.verify(() -> {
                    assertThat(buffer3.toString()).contains("weather of Bengaluru");
                    assertThat(buffer3.toString()).contains("weather of Chandapura");
                    assertThat(buffer3.toString()).contains("weather of Mysore");
                    testContext.completeNow();
                  })));
              })));
          })));
      })));
  }

  @Test
  public void testInsertAPIs(Vertx vertx, VertxTestContext testContext) {
    HttpClient client = vertx.createHttpClient();

    client.request(HttpMethod.GET, 7080, "localhost", "/api/v1/savedata/place/jaipur")
      .compose(req -> req.send().compose(HttpClientResponse::body))
      .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
        assertThat(buffer.toString()).contains("Got Entry for Jaipur");
        testContext.completeNow();
      })));
  }

  @Test
  public void testSearchAPIWithPlace(Vertx vertx, VertxTestContext testContext) {
    HttpClient client = vertx.createHttpClient();

    client.request(HttpMethod.GET, 7080, "localhost", "/api/v1/savedata/place/Bangalore")
      .compose(req -> req.send().compose(HttpClientResponse::body))
      .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
        assertThat(buffer.toString()).contains("Got Entry for Bengaluru");
        //testContext.completeNow();
        client.request(HttpMethod.GET, 7080, "localhost", "/api/v1/search/place/Bengaluru")
          .compose(req -> req.send().compose(HttpClientResponse::body))
          .onComplete(testContext.succeeding(buffer1 -> testContext.verify(() -> {
            assertThat(buffer1.toString()).contains("weather of Bengaluru");
            testContext.completeNow();
          })));
      })));
  }

  @Test
  public void testSearchAPIsWithLatLon(Vertx vertx, VertxTestContext testContext) {
    HttpClient client = vertx.createHttpClient();

    client.request(HttpMethod.GET, 7080, "localhost", "/api/v1/savedata/place/Bengaluru")
      .compose(req -> req.send().compose(HttpClientResponse::body))
      .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
        assertThat(buffer.toString()).contains("Got Entry for Bengaluru");
        //testContext.completeNow();
        client.request(HttpMethod.GET, 7080, "localhost", "/api/v1/search/latlon/12.9762/77.6033")
          .compose(req -> req.send().compose(HttpClientResponse::body))
          .onComplete(testContext.succeeding(buffer1 -> testContext.verify(() -> {
            assertThat(buffer1.toString()).contains("weather of Bengaluru");
            testContext.completeNow();
          })));
      })));
  }

  //Reset the DB before every test case execution
  private static void refershDB(Vertx vertx)
  {
    MySQLConnectOptions connectOptions = new MySQLConnectOptions()
      .setPort(3306)
      .setHost("localhost")
      .setDatabase("weather")
      .setUser("root")
      .setPassword("Bhanu@1234");
    PoolOptions poolOptions = new PoolOptions()
      .setMaxSize(10);

    MySQLPool client2 = MySQLPool.pool(vertx, connectOptions, poolOptions);
    client2.getConnection();
    client2
      .preparedQuery(
        "Truncate weather_data;")
      .execute( ar -> {
      });
  }
}
