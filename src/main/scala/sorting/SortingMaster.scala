package sorting

import java.net.InetAddress
import com.typesafe.scalalogging.Logger
import com.sorting.protos.sorting.{GreeterGrpc, HelloReply, HelloRequest}
import io.grpc.{Server, ServerBuilder}

import scala.concurrent.{ExecutionContext, Future}

object SortingMaster {

  def main(args: Array[String]): Unit = {
    val server = new SortingMaster(ExecutionContext.global)
    server.start()
    server.blockUntilShutdown()
  }

  private val host = InetAddress.getLocalHost.getHostAddress
  private val port = 7001
  private val address = s"${host}:${port}"
}

class SortingMaster(executionContext: ExecutionContext) { self =>
  private val logger = Logger(classOf[SortingSlave])
  private[this] var server: Server = null

  private def start(): Unit = {
    server = ServerBuilder.forPort(SortingMaster.port).addService(GreeterGrpc.bindService(new GreeterImpl, executionContext)).build.start
    logger.debug("debug")
    logger.info("info")
    logger.error("error")
    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      self.stop()
      System.err.println("*** server shut down")
    }
  }

  private def stop(): Unit = {
    if (server != null) {
      server.shutdown()
    }
  }

  private def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }

  private class GreeterImpl extends GreeterGrpc.Greeter {
    override def sayHello(req: HelloRequest) = {
      logger.info(s"sayHello is called : $req.name")
      val reply = HelloReply(message = "Hello " + req.name)
      Future.successful(reply)
    }
  }

}



