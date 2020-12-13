package sorting

import java.net.{InetAddress, URI}
import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.Logger
import com.sorting.protos.sorting._
import io.grpc.{ManagedChannelBuilder, Server, ServerBuilder}
import sorting.SortingMaster.{numberOfSlaves, slaves}

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

object SortingMaster {
  private val logger = Logger(classOf[SortingMaster])
  private val host = InetAddress.getLocalHost.getHostAddress
  private val port = 7001
  private val address = s"${host}:${port}"

  // state variables
  var numberOfSlaves = 0

  // master
  var masterServer: SortingMaster = null

  // slaves
  var slavePorts = new ListBuffer[Int]()
  var slaves = new ListBuffer[SlaveServer]()


  def main(args: Array[String]): Unit = {
    parseArguments(args)
    startMasterServer()

  }

  def parseArguments(args: Array[String]): Unit = {
    numberOfSlaves = args(0).toInt

    for (i <- 1 to numberOfSlaves) {
      slavePorts += 7010 + i
    }

    logger.info(s"parseArguments done : $numberOfSlaves")
  }

  def startMasterServer(): Unit = {
    masterServer = new SortingMaster(logger)
    masterServer.start()
    masterServer.blockUntilShutdown()
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
    logger.info(s"Master starting: ${SortingMaster.address}")

    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      this.shutdown()
      System.err.println("*** server shut down")
    }
  }

  def createConnectionToSlave(uri: URI): SlaveServer = {
    val channel = ManagedChannelBuilder.forAddress(uri.getHost, uri.getPort).usePlaintext().build
    val stub = MasterToSlaveGrpc.blockingStub(channel)
    new SlaveServer(uri, channel, stub)
  }

  def sendInitCompleted(): Unit = {
    if (SortingMaster.slaves.length < SortingMaster.numberOfSlaves) {
      return
    }

    logger.info("sendInitCompleted start")
//    val request = GetSamplingDataRequest()
//
//    for (slave <- slaves) {
//      val response = slave.stub.getSamplingData(request)
//      logger.info(s"sendSamplingStart to ${slave.uri.getPort}")
//    }
  }

  private class SlaveToMasterImpl extends SlaveToMasterGrpc.SlaveToMaster {
    override def getSlavePort(request: GetSlavePortRequest): Future[GetSlavePortReply] = {
      logger.info("getSlavePort start")
      val reply = GetSlavePortReply(port = SortingMaster.slavePorts(0))
      SortingMaster.slavePorts = SortingMaster.slavePorts.drop(1)
      logger.info(s"done : remaining ports ${SortingMaster.slavePorts.length}")
      Future.successful(reply)
    }

    override def sendIntroduce(request: SendIntroduceRequest): Future[SendIntroduceReply] = {
      logger.info("sendIntroduce start")
      val uri = new URI(s"any://${request.host}:${request.port}")
      SortingMaster.slaves += createConnectionToSlave(uri)
      logger.info(s"done : connect to ${uri.getHost}:${uri.getPort} , ${SortingMaster.slaves.length}")
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



