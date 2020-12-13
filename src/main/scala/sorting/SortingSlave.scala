package sorting

import java.net.{InetAddress, URI}
import java.util.concurrent.TimeUnit

import com.sorting.protos.sorting.GreeterGrpc.GreeterBlockingStub
import com.sorting.protos.sorting.{GreeterGrpc, HelloReply, HelloRequest}
import com.typesafe.scalalogging.Logger
import io.grpc.{ManagedChannel, ManagedChannelBuilder, Server, ServerBuilder, StatusRuntimeException}

import scala.concurrent.{ExecutionContext, Future}

object SortingSlave {
  private val logger = Logger(classOf[SortingSlave])

  // state variables
  var inputDirs: List[String] = null
  var outputDir: String = null

  // master
  var masterURI: URI = null
  var master: ProtoServer = null

  // slave
  var slaveServer: SortingSlave = null


  def main(args: Array[String]): Unit = {
    parseArguments(args)
    createConnectionToMaster()

    try {
      slaveServer = new SortingSlave(master, logger)
      slaveServer.greet("Kisukt")
    } finally {
      slaveServer.shutdown()
    }
  }

  def parseArguments(args: Array[String]): Unit = {
    masterURI = new URI("any://" + args(0))

    val indexI = args.indexOf("-I")
    val indexO = args.indexOf("-O", indexI)
    inputDirs = args.slice(indexI + 1, indexO).toList
    outputDir = args(indexO + 1)
    logger.info("parseArguments done")
  }

  def createConnectionToMaster(): Unit = {
    val channel = ManagedChannelBuilder.forAddress(masterURI.getHost, masterURI.getPort).usePlaintext().build
    val stub = GreeterGrpc.blockingStub(channel)
    master = new ProtoServer(masterURI, channel, stub)
    logger.info("createMasterChannel done")
  }
}

class SortingSlave private(master: ProtoServer, logger: Logger) {
  private[this] var server: Server = null

  private def shutdown(): Unit = {
    master.channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
    if (server != null) {
      server.shutdown()
    }
  }

  private def start(): Unit = {
    server = ServerBuilder
      .forPort(1111)
      .addService(GreeterGrpc.bindService(new GreeterImpl, ExecutionContext.global))
      .build.start

    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      this.shutdown()
      System.err.println("*** server shut down")
    }
  }







  /** Say hello to server. */
  def greet(name: String): Unit = {
    logger.info(s"Will try to greet $name ...")
    val request = HelloRequest(name = name)
    try {
      val response = master.stub.sayHelloYa(request)
      logger.info(s"Greeting: $response.message")
    }
    catch {
      case e: StatusRuntimeException =>
        logger.error(s"RPC failed: ${e.getStatus}")
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
