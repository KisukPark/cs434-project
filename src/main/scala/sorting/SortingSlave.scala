package sorting

import java.util.concurrent.TimeUnit
import java.util.logging.{Level, Logger}

import com.sorting.protos.sorting.{GreeterGrpc, HelloRequest}
import com.sorting.protos.sorting.GreeterGrpc.GreeterBlockingStub
import io.grpc.{ManagedChannel, ManagedChannelBuilder, StatusRuntimeException}

object SortingSlave {
  def apply(host: String, port: Int): SortingSlave = {
    val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
    val blockingStub = GreeterGrpc.blockingStub(channel)
    new SortingSlave(channel, blockingStub)
  }

  def main(args: Array[String]): Unit = {
    val client = SortingSlave("localhost", 7001)
    try {
      val user = args.headOption.getOrElse("world")
      client.greet(user)
    } finally {
      client.shutdown()
    }
  }
}

class SortingSlave private(
                                private val channel: ManagedChannel,
                                private val blockingStub: GreeterBlockingStub
                              ) {
  private[this] val logger = Logger.getLogger(classOf[SortingSlave].getName)

  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }

  /** Say hello to server. */
  def greet(name: String): Unit = {
    logger.info("Will try to greet " + name + " ...")
    val request = HelloRequest(name = name)
    try {
      val response = blockingStub.sayHello(request)
      logger.info("Greeting: " + response.message)
    }
    catch {
      case e: StatusRuntimeException =>
        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
    }
  }
}
