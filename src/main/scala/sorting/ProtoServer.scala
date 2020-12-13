package sorting

import java.net.{URI}

import com.sorting.protos.sorting.GreeterGrpc.GreeterBlockingStub
import io.grpc.ManagedChannel

class ProtoServer(var uri: URI, var channel: ManagedChannel, var stub: GreeterBlockingStub) {

}

