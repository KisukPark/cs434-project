package sorting

import java.io.{File, FileReader, LineNumberReader}
import java.net.{InetAddress, URI}
import java.util.concurrent.TimeUnit

import com.sorting.protos.sorting._
import com.typesafe.scalalogging.Logger
import io.grpc.{ManagedChannelBuilder, Server, ServerBuilder}
import sorting.SortingSlave.{myIndex, myPort, otherSlaves}

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

object SortingSlave {
  private val logger = Logger(classOf[SortingSlave])

  // state variables
  var inputDirs: List[String] = null
  var outputDir: String = null
  var partitionTable: Seq[String] = null

  // master
  var masterURI: URI = null
  var master: MasterServer = null

  // slave
  var myAddress: String = null
  var myPort: Int = 0
  var myIndex: Int = 0
  var slaveServer: SortingSlave = null
  var slaveHostTable: Seq[String] = null
  var otherSlaves = new ListBuffer[SlaveServer]()

  def main(args: Array[String]): Unit = {
    parseArguments(args)
    createConnectionToMaster()
    startSlaveServer()

  }

  def startSlaveServer(): Unit = {
    slaveServer = new SortingSlave(master, logger)
    slaveServer.start()
    slaveServer.blockUntilShutdown()
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
    val stub = SlaveToMasterGrpc.blockingStub(channel)
    master = new MasterServer(masterURI, channel, stub)
    logger.info("createMasterChannel done")
  }
}

class SortingSlave private(master: MasterServer, logger: Logger) {
  private[this] var server: Server = null

  private def shutdown(): Unit = {
    master.channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
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
    val portResponse = master.stub.getSlavePort(GetSlavePortRequest())
    SortingSlave.myPort = portResponse.port
    SortingSlave.myAddress = s"${InetAddress.getLocalHost.getHostAddress}:${SortingSlave.myPort}"
    server = ServerBuilder
      .forPort(SortingSlave.myPort)
      .addService(MasterToSlaveGrpc.bindService(new MasterToSlaveImpl, ExecutionContext.global))
      .build.start

    master.stub.sendIntroduce(SendIntroduceRequest(host = InetAddress.getLocalHost.getHostAddress, port = portResponse.port))

    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      this.shutdown()
      System.err.println("*** server shut down")
    }
  }

  private class MasterToSlaveImpl extends MasterToSlaveGrpc.MasterToSlave {
    override def sendInitCompleted(request: SendInitCompletedRequest): Future[SendInitCompletedReply] = {
      SortingSlave.slaveHostTable = request.slaveHostTable
      SortingSlave.myIndex = SortingSlave.slaveHostTable.indexOf(SortingSlave.myAddress)
      SortingSlave.slaveHostTable.foreach(host => {
        if (host != SortingSlave.myAddress) {
          val uri = new URI("any://" + host)
          val channel = ManagedChannelBuilder.forAddress(uri.getHost, uri.getPort).usePlaintext().build
          val stub = SlaveToSlaveGrpc.blockingStub(channel)
          otherSlaves += new SlaveServer(0, uri, channel, null, stub)
          logger.info(s"create connect to ${uri.getPort}")
        }
      })
      Future.successful(SendInitCompletedReply())
    }

    override def getSamplingData(request: GetSamplingDataRequest): Future[GetSamplingDataReply] = {
      val dir = new File(SortingSlave.inputDirs(SortingSlave.myIndex))
      val samplingFileName = dir.listFiles()(0)
      logger.info(s"getSamplingData from ${samplingFileName.getPath}")

      val lineNumberReader = new LineNumberReader(new FileReader(samplingFileName.getPath))
      val keys = (1 to 10000).map(_ => lineNumberReader.readLine().split(" ")(0)).toSeq
      val response = GetSamplingDataReply(keys = keys)
      Future.successful(response)
    }

    override def sendPartitionTable(request: SendPartitionTableRequest): Future[SendPartitionTableReply] = {
      SortingSlave.partitionTable = request.partitionTable
      logger.info(s"receive partition table ${SortingSlave.partitionTable.toString}")
      Future.successful(SendPartitionTableReply())
    }

    override def sendPartitionStart(request: SendPartitionStartRequest): Future[SendPartitionStartReply] = ???

    override def sendShufflingStart(request: SendShufflingStartRequest): Future[SendShufflingStartReply] = ???

    override def sendMergeStart(request: SendMergeStartRequest): Future[SendMergeStartReply] = ???
  }
}
