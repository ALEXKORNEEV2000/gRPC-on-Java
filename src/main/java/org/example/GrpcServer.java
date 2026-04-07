package org.example;

import io.grpc.Server;
import io.grpc.ServerBuilder;

public final class GrpcServer {

    private GrpcServer() {
    }

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(env("GRPC_PORT", "9090"));

        Server server = ServerBuilder.forPort(port)
                .addService(new KVServiceImpl())
                .build()
                .start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.shutdown();
            TarantoolConnection.close();
        }));

        System.out.println("gRPC server started on port " + port);
        server.awaitTermination();
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? def : v;
    }
}