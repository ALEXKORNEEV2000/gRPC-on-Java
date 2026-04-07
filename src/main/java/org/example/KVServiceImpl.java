package org.example;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.example.grpc.CountResponse;
import org.example.grpc.DeleteRequest;
import org.example.grpc.Empty;
import org.example.grpc.GetRequest;
import org.example.grpc.GetResponse;
import org.example.grpc.KVPair;
import org.example.grpc.KVServiceGrpc;
import org.example.grpc.PutRequest;
import org.example.grpc.RangeRequest;
import org.example.KVEntry;

public class KVServiceImpl extends KVServiceGrpc.KVServiceImplBase {

    private final KVRepository repository = new KVRepository();

    @Override
    public void put(PutRequest request, StreamObserver<Empty> responseObserver) {
        try {
            repository.put(
                    request.getKey(),
                    request.getHasValue() ? request.getValue().toByteArray() : null,
                    request.getHasValue()
            );
            responseObserver.onNext(Empty.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(
                    Status.INTERNAL.withDescription("put failed: " + e.getMessage())
                            .withCause(e)
                            .asRuntimeException()
            );
        }
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        try {
            KVEntry entry = repository.get(request.getKey());

            GetResponse.Builder b = GetResponse.newBuilder();
            if (entry == null) {
                b.setFound(false).setHasValue(false);
            } else {
                b.setFound(true).setHasValue(entry.hasValue());
                if (entry.hasValue()) {
                    b.setValue(ByteString.copyFrom(entry.value()));
                }
            }

            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(
                    Status.INTERNAL.withDescription("get failed: " + e.getMessage())
                            .withCause(e)
                            .asRuntimeException()
            );
        }
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<Empty> responseObserver) {
        try {
            repository.delete(request.getKey());
            responseObserver.onNext(Empty.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(
                    Status.INTERNAL.withDescription("delete failed: " + e.getMessage())
                            .withCause(e)
                            .asRuntimeException()
            );
        }
    }

    @Override
    public void range(RangeRequest request, StreamObserver<KVPair> responseObserver) {
        try {
            ServerCallStreamObserver<KVPair> serverObserver =
                    responseObserver instanceof ServerCallStreamObserver<KVPair>
                            ? (ServerCallStreamObserver<KVPair>) responseObserver
                            : null;

            repository.range(request.getKeySince(), request.getKeyTo(), entry -> {
                if (serverObserver != null && serverObserver.isCancelled()) {
                    return;
                }

                KVPair.Builder b = KVPair.newBuilder()
                        .setKey(entry.key())
                        .setHasValue(entry.hasValue());

                if (entry.hasValue()) {
                    b.setValue(ByteString.copyFrom(entry.value()));
                }

                responseObserver.onNext(b.build());
            });

            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(
                    Status.INTERNAL.withDescription("range failed: " + e.getMessage())
                            .withCause(e)
                            .asRuntimeException()
            );
        }
    }

    @Override
    public void count(Empty request, StreamObserver<CountResponse> responseObserver) {
        try {
            long count = repository.count();
            responseObserver.onNext(CountResponse.newBuilder().setCount(count).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(
                    Status.INTERNAL.withDescription("count failed: " + e.getMessage())
                            .withCause(e)
                            .asRuntimeException()
            );
        }
    }
}