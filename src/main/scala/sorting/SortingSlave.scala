package sorting

import java.net.URI
import java.util.concurrent.TimeUnit

import com.sorting.protos.sorting.GreeterGrpc.GreeterBlockingStub
import com.sorting.protos.sorting.{GreeterGrpc, HelloRequest}
import com.typesafe.scalalogging.Logger
import io.grpc.{ManagedChannel, ManagedChannelBuilder, StatusRuntimeException}

object SortingSlave {

  var channel: ManagedChannel = null
  var blockingStub: GreeterBlockingStub = null

  var inputDirs: List[String] = null
  var outputDir: String = null


  def apply(host: String, port: Int): SortingSlave = {
    new SortingSlave(channel, blockingStub)
  }

  def main(args: Array[String]): Unit = {
    // args
    val uri = new URI("any://" + args(0))
    val host = uri.getHost
    val port = uri.getPort
    channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
    blockingStub = GreeterGrpc.blockingStub(channel)
    println(args(0))

    val indexI = args.indexOf("-I")
    val indexO = args.indexOf("-O", indexI)
    inputDirs = args.slice(indexI + 1, indexO).toList
    outputDir = args(indexO + 1)

    println(inputDirs.toString)
    println(outputDir)

    val client = new SortingSlave(channel, blockingStub)

    try {
      val user = args.headOption.getOrElse("world")
      client.greet(user)
    } finally {
      client.shutdown()
    }
  }
}

class SortingSlave private(channel: ManagedChannel, blockingStub: GreeterBlockingStub) {

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
