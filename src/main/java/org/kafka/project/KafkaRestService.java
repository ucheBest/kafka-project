package org.kafka.project;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Router;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaRestService extends AbstractVerticle {
    private final KafkaRestRouteHandler routeHandler = new KafkaRestRouteHandler();

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

        router.get("/kafka/publish/allData").handler(routeHandler::publishAllData);

        router.get("/kafka/topic/:topic_name/:offset/:count").handler(routeHandler::consumeDataFromOffSet);

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
