package com.demo.weatherApp;


import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;

public class SensorVerticle extends AbstractVerticle {

  @Override
  public void start(Promise<Void> startPromise) throws Exception {

    Router router = Router.router(vertx);
    String url = "api.openweathermap.org";
    WebClient client = WebClient.create(vertx);

    MySQLConnectOptions connectOptions = new MySQLConnectOptions()
      .setPort(3306)
      .setHost("localhost")
      .setDatabase("weather")
      .setUser("root")
      .setPassword("Bhanu@1234");

    PoolOptions poolOptions = new PoolOptions()
      .setMaxSize(10);

    MySQLPool client2 = MySQLPool.pool(vertx, connectOptions, poolOptions);

    // Insert APIs

    //Insert the records as per Place into MYSQL DB
    router.get("/api/v1/savedata/place/:place").handler(ctx -> {

      String place = ctx.pathParam("place");
      String nextUrl = "/data/2.5/weather/?q=" + place + "&appid=361f8f567368bc77c63b5a4bae81ea78";
      client.get(url, nextUrl).as(BodyCodec.jsonObject()).send()
        .onSuccess(response -> {
          JsonObject body2 = response.body();
          String name = body2.getString("name");
          String weather = body2.getString("weather");
          String id = body2.getString("id");
          JsonObject coord = body2.getJsonObject("coord");
          JsonObject sys = body2.getJsonObject("sys");
          String lon = coord.getString("lon");
          String lat = coord.getString("lat");
          String country = sys.getString("country");
          String c_id = sys.getString("id");
          String message = "";

          client2.getConnection();
          client2
            .preparedQuery(
              "INSERT INTO weather_data (name, weather, id, lat, lon, country,c_id) VALUES (?,?,?,?,?,?,?);")
            .execute(Tuple.of(name, weather, id, lat, lon, country, c_id), ar -> {
              if (ar.succeeded()) {
                ctx.response().end("Got Entry for " + name);
              } else {
                ctx.response().end("Failure: " + ar.cause().getMessage());
              }
            });
        })
        .onFailure(err -> ctx.response().end("Place Not Found: " + place));
    });

    //Insert the records as per country and zipcode into MYSQL DB. Different country can have same Zip,
    // so doing it to the combination of zip and country
    router.get("/api/v1/savedata/zip/:country/:zipcode").handler(ctx -> {
      String zipcode = ctx.pathParam("zipcode");
      String countryCode = ctx.pathParam("country");

      String nextUrl = "/data/2.5/weather/?zip=" + zipcode + "," + countryCode
        + "&appid=361f8f567368bc77c63b5a4bae81ea78";
      client.get(url, nextUrl).as(BodyCodec.jsonObject()).send()
        .onSuccess(response -> {
          JsonObject body2 = response.body();
          String name = body2.getString("name");
          String weather = body2.getString("weather");
          String id = body2.getString("id");
          JsonObject coord = body2.getJsonObject("coord");
          JsonObject sys = body2.getJsonObject("sys");
          String lon = coord.getString("lon");
          String lat = coord.getString("lat");
          String country = sys.getString("country");
          String c_id = sys.getString("id");

          client2.getConnection();
          client2
            .preparedQuery(
              "INSERT INTO weather_data (name, weather, id, lat, lon, country, c_id) VALUES (?,?,?,?,?,?,?);")
            .execute(Tuple.of(name, weather, id, lat, lon, country, c_id), ar -> {
              if (ar.succeeded()) {
                System.out.println("Got Entry ");
                ctx.response().end("Got Entry for zipcode : " + zipcode);
              } else {
                ctx.response().end("Failure: " + ar.cause().getMessage());
              }
            });
        })
        .onFailure(err -> ctx.response().end("Zip Not Found: " + zipcode));
    });

    //Insert the records as per latlon into MYSQL DB
    router.get("/api/v1/savedata/latlon/:lat/:lon").handler(ctx -> {
      String lat = ctx.pathParam("lat");
      String lon = ctx.pathParam("lon");
      String nextUrl = "/data/2.5/weather/?lat=" + lat + "&lon=" + lon + "&appid=361f8f567368bc77c63b5a4bae81ea78";
      client.get(url, nextUrl).as(BodyCodec.jsonObject()).send()
        .onSuccess(response -> {
          JsonObject body2 = response.body();

          String name = body2.getString("name");
          String weather = body2.getString("weather");
          String id = body2.getString("id");
          JsonObject coord = body2.getJsonObject("coord");
          JsonObject sys = body2.getJsonObject("sys");
          String lo = coord.getString("lon");
          String la = coord.getString("lat");
          String country = sys.getString("country");
          String c_id = sys.getString("id");

          client2.getConnection();
          client2
            .preparedQuery(
              "INSERT INTO weather_data (name, weather, id, lat, lon, country, c_id) VALUES (?,?,?,?,?,?,?);")
            .execute(Tuple.of(name, weather, id, la, lo, country, c_id), ar -> {
              if (ar.succeeded()) {
                System.out.println("Got Entry ");
                ctx.response().end("Got Entry for lat " + lat + ", lon" + lon);
              } else {
                ctx.response().end("Failure: " + ar.cause().getMessage());
              }
            });
        })
        .onFailure(err -> ctx.response().end("LatLon not found : lat " + lat + " lon" + lon));
    });

    //Search APIs

    //Search the record using place from MySQL DB
    router.get("/api/v1/search/place/:place").handler(ctx -> {
      ctx.vertx().setTimer(5000, tid -> {
        ctx.response().end("Request Timeout");
      });
      String place = ctx.pathParam("place");
      client2.getConnection();
      client2
        .preparedQuery(
          "SELECT * FROM  weather_data WHERE name=?;")
        .execute(Tuple.of(place), ar -> {
          if (ar.succeeded()) {
            String show = "";
            RowSet<Row> rows = ar.result();
            for (Row row : rows) {
              show += "weather of ";
              // System.out.print("weather of " );
              for (int i = 0; i < 7; i++) {
                show += row.getValue(i);
                show += " ";
              }
              System.out.print("\n");
            }
            System.out.println("Got weather data of " + place + " " + ar.result() + " " + show);
            ctx.response().end(show);

          } else {
            System.out.println("Failure: " + ar.cause().getMessage());
            ctx.response().end("Place Not Found: " + place);
            client2.close();
          }
        });

    });


    //Search the record using lat lon from MySQL DB
    router.get("/api/v1/search/latlon/:lat/:lon").handler(ctx -> {
      ctx.vertx().setTimer(5000, tid -> {
        ctx.response().end("Request Timeout");
      });
      String lat = ctx.pathParam("lat");
      String lon = ctx.pathParam("lon");
      client2.getConnection();
      client2
        .preparedQuery(
          "SELECT * FROM  weather_data WHERE CAST(lat AS DECIMAL) = CAST(? AS DECIMAL) AND CAST(lon AS DECIMAL)=CAST(? AS DECIMAL);")
        .execute(Tuple.of(lat, lon), ar -> {
          if (ar.succeeded()) {
            String show = "";
            RowSet<Row> rows = ar.result();
            for (Row row : rows) {
              show += "weather of ";
              for (int i = 0; i < 7; i++) {
                show += row.getValue(i);
                show += " ";
              }
              System.out.print("\n");
            }
            System.out.println("Got weather data of " + lat + " " + lon + " " + ar.result() + " " + show);
              ctx.response().end(show);

          } else {
            System.out.println("Failure: " + ar.cause().getMessage());
            ctx.response().end("Lat Lon Not Found: " + lat + " " + lon );
            client2.close();
          }
        });

    });

    //Search the records under input range of lan lon from MySQL DB
    router.get("/api/v1/search/latlon/range/:lat/:lon/:range").handler(ctx -> {
      ctx.vertx().setTimer(5000, tid -> {
        ctx.response().end("Request Timeout");
      });
      String lat = ctx.pathParam("lat");
      String lon = ctx.pathParam("lon");
      String range = ctx.pathParam("range");
      client2.getConnection();
      client2
        .preparedQuery(
          "SELECT * FROM  weather_data;")
        .execute(ar -> {
          if (ar.succeeded()) {
            String show = "";
            RowSet<Row> rows = ar.result();
            for (Row row : rows) {
              double distance = distance(Double.parseDouble(lat),row.getFloat(3),Double.parseDouble(lon),row.getFloat(4));
              if(distance < Double.parseDouble(range)) {
                show += "weather of ";
                for (int i = 0; i < 7; i++) {
                  show += row.getValue(i);
                  show += " ";
                }
                System.out.print("\n");
              }
            }
            System.out.println("Got weather data of " + lat + " " + lon + " " + ar.result() + " " + show);
            ctx.response().end(show);

          } else {
            System.out.println("Failure: " + ar.cause().getMessage());
            ctx.response().end("No Record with provided lat lon : " + lat + " " + lon );
            client2.close();
          }
        });

    });

    //Update APIs

    //Instead of gettting the values from User, We can directly pull the data from weather API and update out DB.
    //If we want to get the update the filed using user input then we can ise router.Put/Patch
    //Update the records using place in MySQL from Weather API
    router.get("/api/v1/place/:place/update").handler(ctx -> {

      String place = ctx.pathParam("place");
      String nextUrl = "/data/2.5/weather/?q=" + place + "&appid=361f8f567368bc77c63b5a4bae81ea78";
      client.get(url, nextUrl).as(BodyCodec.jsonObject()).send()
        .onSuccess(response -> {
          JsonObject body2 = response.body();
          String name = body2.getString("name");
          String weather = body2.getString("weather");
          String id = body2.getString("id");

          client2.getConnection();
          client2
            .preparedQuery(
              "UPDATE weather_data SET weather=?, id=? WHERE name=?;")
            .execute(Tuple.of(weather, id, name), ar -> {
              if (ar.succeeded()) {
                System.out.println("Got Entry ");
                ctx.response().end("Update Records with place : " + place );
              } else {
                System.out.println("Failure: " + ar.cause().getMessage());
                ctx.response().end("Failure: " + ar.cause().getMessage());
              }
            });

          String show = body2.toString();
          System.out.println("Rec " + response.statusCode() + " " + show + "\n Response of Place");
          ctx.response().end(Json.encodePrettily(body2));

        })
        .onFailure(err ->  ctx.response().end("Something Went Wrong"));
    });

    //Update the records using zipcode in MySQL from Weather API
    router.get("/api/v1/zip/:country/:zipcode/update").handler(ctx -> {
      String zipcode = ctx.pathParam("zipcode");
      String countryCode = ctx.pathParam("country");

      String nextUrl = "/data/2.5/weather/?zip=" + zipcode + "," + countryCode
        + "&appid=361f8f567368bc77c63b5a4bae81ea78";
      client.get(url, nextUrl).as(BodyCodec.jsonObject()).send()
        .onSuccess(response -> {
          JsonObject body2 = response.body();
          String name = body2.getString("name");
          String weather = body2.getString("weather");
          String id = body2.getString("id");

          client2.getConnection();
          client2
            .preparedQuery(
              "UPDATE weather_data SET weather=?, id=? WHERE name=?;")
            .execute(Tuple.of(weather, id, name), ar -> {
              if (ar.succeeded()) {
                System.out.println("Got Updated ");
                ctx.response().end("Update Records with zipcode : " + zipcode );
              } else {
                System.out.println("Failure: " + ar.cause().getMessage());
                ctx.response().end("Failure: " + ar.cause().getMessage());
              }
            });

          String show = body2.toString();
          System.out.println("Rec " + response.statusCode() + " " + show + "\n Response of zipcode");
          ctx.response().end(Json.encodePrettily(body2));

        })
        .onFailure(err ->  ctx.response().end("Something Went Wrong"));
    });

    //Update the records using lat lon in MySQL from Weather API
    router.get("/api/v1/latlon/:lat/:lon/update").handler(ctx -> {
      String lat = ctx.pathParam("lat");
      String lon = ctx.pathParam("lon");
      String nextUrl = "/data/2.5/weather/?lat=" + lat + "&lon=" + lon + "&appid=361f8f567368bc77c63b5a4bae81ea78";
      client.get(url, nextUrl).as(BodyCodec.jsonObject()).send()
        .onSuccess(response -> {
          JsonObject body2 = response.body();

          String name = body2.getString("name");
          String weather = body2.getString("weather");
          String id = body2.getString("id");
          client2.getConnection();
          client2
            .preparedQuery(
              "UPDATE weather_data SET weather=?, id=? WHERE name=?;")
            .execute(Tuple.of(weather, id, name), ar -> {
              if (ar.succeeded()) {
                System.out.println("Got Entry ");
                ctx.response().end("Update Records with lat lon : " + lat + " " + lon );
              } else {
                System.out.println("Failure: " + ar.cause().getMessage());
                ctx.response().end("Failure: " + ar.cause().getMessage());
              }
            });
          String show = body2.toString();
          System.out.println("Rec " + response.statusCode() + " " + show + "\n Response of lat & long");
          ctx.response().end(Json.encodePrettily(body2));

        })
        .onFailure(err -> ctx.response().end("Something Went Wrong" ));
    });


    // GET APIs

    //Get Data directly from Weather API using place name
    router.get("/api/v1/getdata/place/:place").handler(ctx -> {

      String place = ctx.pathParam("place");
      String nextUrl = "/data/2.5/weather/?q=" + place + "&appid=361f8f567368bc77c63b5a4bae81ea78";
      client.get(url, nextUrl).as(BodyCodec.jsonObject()).send()
        .onSuccess(response -> {
          JsonObject body2 = response.body();
          String show = body2.toString();
          System.out.println("Rec " + response.statusCode() + " " + show + "\n Response of Place");
          ctx.response().end(Json.encodePrettily(body2));

        })
        .onFailure(err ->  ctx.response().end("Something Went Wrong"));
    });

    //Get Data directly from Weather API using zipcode and country
    router.get("/api/v1/getdata/zip/:country/:zipcode").handler(ctx -> {
      String zipcode = ctx.pathParam("zipcode");
      String countryCode = ctx.pathParam("country");

      String nextUrl = "/data/2.5/weather/?zip=" + zipcode + "," + countryCode
        + "&appid=361f8f567368bc77c63b5a4bae81ea78";
      client.get(url, nextUrl).as(BodyCodec.jsonObject()).send()
        .onSuccess(response -> {
          JsonObject body2 = response.body();

          String show = body2.toString();
          System.out.println("Rec " + response.statusCode() + " " + show + "\n Response of zipcode");
          ctx.response().end(Json.encodePrettily(body2));

        })
        .onFailure(err ->  ctx.response().end("Something Went Wrong"));
    });

    //Get Data directly from Weather API using lat lon
    router.get("/api/v1/getdata/latlon/:lat/:lon").handler(ctx -> {
      String lat = ctx.pathParam("lat");
      String lon = ctx.pathParam("lon");
      String nextUrl = "/data/2.5/weather/?lat=" + lat + "&lon=" + lon + "&appid=361f8f567368bc77c63b5a4bae81ea78";
      client.get(url, nextUrl).as(BodyCodec.jsonObject()).send()
        .onSuccess(response -> {
          JsonObject body2 = response.body();
          String show = body2.toString();
          System.out.println("Rec " + response.statusCode() + " " + show + "\n Response of lat & long");
          ctx.response().end(Json.encodePrettily(body2));
        })
        .onFailure(err -> ctx.response().end("Something Went Wrong"));
    });

    // Configure the application in Port No. 7080
    vertx.createHttpServer().requestHandler(router).listen(7080, http -> {
      if (http.succeeded()) {
        startPromise.complete();
        System.out.println("HTTP server started on port 7080");
      } else {
        startPromise.fail(http.cause());
      }
    });
  }


  public static double distance(double lat1,
                                double lat2, double lon1,
                                double lon2)
  {
    lon1 = Math.toRadians(lon1);
    lon2 = Math.toRadians(lon2);
    lat1 = Math.toRadians(lat1);
    lat2 = Math.toRadians(lat2);

    // Haversine formula
    double dlon = lon2 - lon1;
    double dlat = lat2 - lat1;
    double a = Math.pow(Math.sin(dlat / 2), 2)
      + Math.cos(lat1) * Math.cos(lat2)
      * Math.pow(Math.sin(dlon / 2),2);

    double c = 2 * Math.asin(Math.sqrt(a));

    double r = 6371;

    return(c * r);
  }
}

