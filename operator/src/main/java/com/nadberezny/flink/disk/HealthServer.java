package com.nadberezny.flink.disk;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.javaoperatorsdk.operator.Operator;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;

/** Minimal probe endpoints, so the Deployment can have real liveness/readiness checks. */
final class HealthServer {

    private final HttpServer http;

    HealthServer(int port, Operator operator) throws IOException {
        this.http = HttpServer.create(new InetSocketAddress(port), 0);
        this.http.setExecutor(Executors.newSingleThreadExecutor());

        http.createContext("/healthz", exchange ->
                respond(exchange, operator.getRuntimeInfo().isStarted(), "started"));

        // Unhealthy informers mean we are no longer seeing FlinkDeployment changes, which for this
        // operator is indistinguishable from being down.
        http.createContext("/readyz", exchange -> {
            boolean ready = operator.getRuntimeInfo().isStarted()
                    && operator.getRuntimeInfo().allEventSourcesAreHealthy();
            respond(exchange, ready, "event sources healthy");
        });
    }

    void start() {
        http.start();
    }

    void stop() {
        http.stop(0);
    }

    private static void respond(HttpExchange exchange, boolean ok, String what) throws IOException {
        byte[] body = ((ok ? "ok: " : "not ok: ") + what + "\n").getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain");
        exchange.sendResponseHeaders(ok ? 200 : 503, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }
}
