package sorting

import java.net.{InetAddress, URI}
import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.Logger
import com.sorting.protos.sorting._
import io.grpc.{ManagedChannelBuilder, Server, ServerBuilder}
import sorting.SortingMaster.{numberOfSlaves, partitionTable, slaves}

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.io.Path
import scala.util.Success

object SortingMaster {
  private val logger = Logger(classOf[SortingMaster])
  private val host = InetAddress.getLocalHost.getHostAddress
  private val port = 7001
  private val address = s"${host}:${port}"

  // state variables
  var numberOfSlaves = 0
  var partitionTable: Seq[String] = Seq()

  // master
  var masterServer: SortingMaster = null

  // slaves
  var slavePorts = new ListBuffer[Int]()
  var slaves = new ListBuffer[SlaveServer]()


  def main(args: Array[String]): Unit = {
    parseArguments(args)
    cleanTempDir()
    startMasterServer()
  }

  def parseArguments(args: Array[String]): Unit = {
    numberOfSlaves = args(0).toInt

    for (i <- 1 to numberOfSlaves) {
      slavePorts += 7010 + i
    }
  }

  def startMasterServer(): Unit = {
    masterServer = new SortingMaster(logger)
    masterServer.start()
    masterServer.blockUntilShutdown()
  }

  def cleanTempDir(): Unit = {
    val tempPath: Path = Path("./temp")
    tempPath.deleteRecursively()
  }
}

class SortingMaster(logger: Logger) { self =>
  private[this] var server: Server = null

  private def shutdown(): Unit = {
    for (slave <- SortingMaster.slaves) {
      slave.channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
    }
    if (server != null) {
      server.shutdown()
    }
  }

  private def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }

  private def start(): Unit = {
    server = ServerBuilder
      .forPort(SortingMaster.port)
      .addService(SlaveToMasterGrpc.bindService(new SlaveToMasterImpl, ExecutionContext.global))
      .build.start
    logger.info(s"Master starting  ${SortingMaster.address}")

    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      this.shutdown()
      System.err.println("*** server shut down")
    }
  }

  def createConnectionToSlave(uri: URI): SlaveServer = {
    val channel = ManagedChannelBuilder.forAddress(uri.getHost, uri.getPort).usePlaintext().build
    val stub = MasterToSlaveGrpc.blockingStub(channel)
    new SlaveServer(SortingMaster.slaves.length, uri, channel, stub, null)
  }

  def sendInitCompleted(): Unit = {
    if (SortingMaster.slaves.length < SortingMaster.numberOfSlaves) {
      return
    }

    val request = SendInitCompletedRequest(slaveHostTable = slaves.toSeq.map(slave => s"${slave.uri.getHost}:${slave.uri.getPort}"))
    slaves.foreach(slave => {
      val _ = slave.fromMasterStub.sendInitCompleted(request)
    })

    logger.info("Complete init step")
    this.getSamplingData()
  }

  def getSamplingData(): Unit = {
    logger.info("Get sampling data from slaves")
    var keys: Seq[String] = Seq()
    slaves.foreach(slave => {
      val request = GetSamplingDataRequest()
      val response = slave.fromMasterStub.getSamplingData(request)
      keys = keys ++ response.keys
    })
    keys = keys.filter(str => !str.isEmpty).sorted

    // create partition table
    val gap = keys.length / SortingMaster.numberOfSlaves
    (1 to SortingMaster.numberOfSlaves - 1).foreach(i => {
      SortingMaster.partitionTable = SortingMaster.partitionTable :+ keys(i * gap)
    })

    logger.info(s"Generate partitioning table ${SortingMaster.partitionTable.toString}")

    slaves.foreach(slave => {
      val request = SendPartitionTableRequest(partitionTable = SortingMaster.partitionTable)
      val response = slave.fromMasterStub.sendPartitionTable(request)
    })

    this.sendPartitionStart()
  }

  def sendPartitionStart(): Unit = {
    slaves.foreach(slave => {
      val request = SendPartitionStartRequest()
      val response = slave.fromMasterStub.sendPartitionStart(request)
    })
    // TODO: to next step
  }

  private class SlaveToMasterImpl extends SlaveToMasterGrpc.SlaveToMaster {
    override def getSlavePort(request: GetSlavePortRequest): Future[GetSlavePortReply] = {
      val reply = GetSlavePortReply(port = SortingMaster.slavePorts(0))
      SortingMaster.slavePorts = SortingMaster.slavePorts.drop(1)
      Future.successful(reply)
    }

    override def sendIntroduce(request: SendIntroduceRequest): Future[SendIntroduceReply] = {
      val uri = new URI(s"any://${request.host}:${request.port}")
      SortingMaster.slaves += createConnectionToSlave(uri)
      logger.info(s"Connect to slave ${uri.getHost}:${uri.getPort}, waiting ${SortingMaster.numberOfSlaves - SortingMaster.slaves.length} more...")
      Future
        .successful(SendIntroduceReply())
        .andThen({
          case Success(_) => sendInitCompleted()
        })
    }
  }

//  private class GreeterImpl extends GreeterGrpc.Greeter {
//    override def sayHelloYa(req: HelloRequest) = {
//      logger.info(s"sayHello is called : $req.name")
//
//      SortingMaster.numberOfSlaves -= 1
//
//      while (SortingMaster.numberOfSlaves > 0) {
//        // 모든 slave 로 부터 샘플 데이터가 올때 까지 대기
//      }
//
//      val reply = HelloReply(message = "Hello " + req.name)
//      Future.successful(reply)
//    }
//  }

}



