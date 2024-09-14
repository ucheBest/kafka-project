package org.kafka.project;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.Json;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaRestService extends AbstractVerticle {
    private final KafkaProducerMain producer = new KafkaProducerMain();
    private final KafkaConsumerMain consumer = new KafkaConsumerMain();

    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(new KafkaRestService(), res -> {
            if (res.succeeded()) {
                log.info("Successfully Deployed Service!");
            } else {
                log.error("Failed to deploy Service. Error: ", res.cause());
            }
        });
    }

    @Override
    public void start(Promise<Void> startPromise) {
        final Router router = Router.router(vertx);

        router.route().handler(BodyHandler.create());

        router.get("/kafka/publish/allData").handler(ctx -> {
            this.producer.publishAllData();
            ctx.response()
                    .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                    .setStatusCode(200).end("Successfully published all records");
        });

        router.get("/kafka/topic/:topic_name/:offset/:count").handler(ctx -> {
            String topic_name = ctx.pathParam("topic_name");
            String offSet = ctx.pathParam("offset");
            String num = ctx.pathParam("count");
            this.consumer.getFromOffSet(topic_name, Integer.parseInt(offSet), Integer.parseInt(num))
                    .onSuccess(res -> {
                        String response = Json.encode(res);
                        ctx.response()
                                .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                                .setStatusCode(200).end(response);

                    })
                    .onFailure(error -> {
                        ctx.response()
                                .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                                .setStatusCode(500).end("An unexpected error occurred, please contact support");
                    });
        });

        HttpServerOptions options = new HttpServerOptions().setPort(3003);
        HttpServer httpServer = vertx.createHttpServer(options);
        httpServer.requestHandler(router);

        httpServer.listen(ar -> {
            if (ar.succeeded()) {
                log.info("Listening on port 3003");
                startPromise.complete();
            } else {
                startPromise.fail(ar.cause());
            }
        });
    }
}
