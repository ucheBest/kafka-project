package org.kafka.project;

import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.Json;
import io.vertx.ext.web.RoutingContext;

public class KafkaRestRouteHandler {
    private final KafkaProducerMain producer = new KafkaProducerMain();
    private final KafkaConsumerMain consumer = new KafkaConsumerMain();

    public void publishAllData(RoutingContext ctx) {
        this.producer.publishAllData();
        ctx.response()
                .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .setStatusCode(200).end("Successfully published all records");
    }

    public void consumeDataFromOffSet(RoutingContext ctx) {
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
    }
}
