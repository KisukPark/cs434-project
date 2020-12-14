package sorting

import java.net.URI

import com.sorting.protos.sorting.MasterToSlaveGrpc.MasterToSlaveBlockingStub
import com.sorting.protos.sorting.SlaveToMasterGrpc.SlaveToMasterBlockingStub
import com.sorting.protos.sorting.SlaveToSlaveGrpc.SlaveToSlaveBlockingStub
import io.grpc.ManagedChannel

class MasterServer(var uri: URI, var channel: ManagedChannel, var stub: SlaveToMasterBlockingStub) {

}

class SlaveServer(var index: Int, var uri: URI, var channel: ManagedChannel, var fromMasterStub: MasterToSlaveBlockingStub, var fromSlaveStub: SlaveToSlaveBlockingStub) {

}

