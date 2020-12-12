package sorting

import java.util.concurrent.TimeUnit

import com.sorting.protos.sorting.GreeterGrpc.GreeterBlockingStub
import com.sorting.protos.sorting.{GreeterGrpc, HelloRequest}
import com.typesafe.scalalogging.Logger
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
  private val logger = Logger(classOf[SortingSlave])

  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }

  /** Say hello to server. */
  def greet(name: String): Unit = {
    logger.info(s"Will try to greet $name ...")
    val request = HelloRequest(name = name)
    try {
      val response = blockingStub.sayHello(request)
      logger.info(s"Greeting: $response.message")
    }
    catch {
      case e: StatusRuntimeException =>
        logger.error(s"RPC failed: ${e.getStatus}")
    }
  }
}
