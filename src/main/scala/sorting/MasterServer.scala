package sorting

import java.net.URI

import com.sorting.protos.sorting.MasterToSlaveGrpc.MasterToSlaveBlockingStub
import com.sorting.protos.sorting.SlaveToMasterGrpc.SlaveToMasterBlockingStub
import io.grpc.ManagedChannel

class MasterServer(var uri: URI, var channel: ManagedChannel, var stub: SlaveToMasterBlockingStub) {

}

class SlaveServer(var uri: URI, var channel: ManagedChannel, var stub: MasterToSlaveBlockingStub) {

}

