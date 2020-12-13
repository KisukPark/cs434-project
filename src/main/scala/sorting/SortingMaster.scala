package sorting

import java.net.InetAddress
import java.util.concurrent.TimeUnit

import com.sorting.protos.sorting.GreeterGrpc.GreeterBlockingStub
import com.typesafe.scalalogging.Logger
import com.sorting.protos.sorting.{GreeterGrpc, HelloReply, HelloRequest}
import io.grpc.{ManagedChannel, Server, ServerBuilder}
import sorting.SortingMaster.{logger, slaves}

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

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
  var slaves = new ListBuffer[ProtoServer]()


  def main(args: Array[String]): Unit = {

    parseArguments(args)
    startMasterServer()

  }

  def parseArguments(args: Array[String]): Unit = {
    numberOfSlaves = args(0).toInt
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
      .addService(GreeterGrpc.bindService(new GreeterImpl, ExecutionContext.global))
      .build.start
    logger.info(s"Master starting: ${SortingMaster.address}")

    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      this.shutdown()
      System.err.println("*** server shut down")
    }
  }

  private class GreeterImpl extends GreeterGrpc.Greeter {
    override def sayHelloYa(req: HelloRequest) = {
      logger.info(s"sayHello is called : $req.name")

      SortingMaster.numberOfSlaves -= 1

      while (SortingMaster.numberOfSlaves > 0) {
        // 모든 slave 로 부터 샘플 데이터가 올때 까지 대기
      }

      val reply = HelloReply(message = "Hello " + req.name)
      Future.successful(reply)
    }
  }

}



