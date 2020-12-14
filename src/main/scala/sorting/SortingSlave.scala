package sorting

import java.io.{BufferedWriter, File, FileReader, FileWriter, LineNumberReader}
import java.net.{InetAddress, URI}
import java.util.concurrent.TimeUnit

import com.sorting.protos.sorting._
import com.typesafe.scalalogging.Logger
import io.grpc.{ManagedChannelBuilder, Server, ServerBuilder}
import sorting.SortingSlave.otherSlaves
import util.Utils

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.io.Path

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
//    test()
    parseArguments(args)
    createConnectionToMaster()
    startSlaveServer()
  }

  def test(): Unit = {
//    val dirPath: Path = Path("./temp/bye")
//    dirPath.createDirectory()
//
//    val file = new File("./temp/hi.txt")
//    if (!file.exists()) {
//      file.createNewFile()
//    }
//    val bw = new BufferedWriter(new FileWriter(file))
//    bw.write("hi\n")
//    bw.write("ho\n")
//    bw.close()


    val r = new scala.util.Random
    val sb = new StringBuilder
    for (i <- 1 to 20) {
      sb.append(r.nextPrintableChar)
    }
    logger.info(sb.toString)
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
  private[this] val util = new Utils()

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

  def sortAndParition(filePath: String): Unit = {
    val lineNumberReader = new LineNumberReader(new FileReader(filePath))
    var lines = Seq[String]()
    var line: String = null

    logger.info("read and sort start")

    // read file
    var i = 0
    var j = 0
    line = lineNumberReader.readLine()
    while (line != null) {
      lines = lines :+ line
      line = lineNumberReader.readLine()
      i += 1
      if (i >= 100) {
        logger.info(s"${j*100+i} lines")
        j += 1
        i = 0
      }
    }

    // sort
    lines.sortBy(a => util.getKeyFromLine(a))

    logger.info(s"read and sort done, total ${lines.length} lines")

    // partitioning

    logger.info("partitioning start")
    var writers = Seq[BufferedWriter]()

    SortingSlave.slaveHostTable.foreach(slaveAddress => {
      // create temp directory
      val dirPath: Path = Path(s"./temp/${SortingSlave.myAddress}/${slaveAddress}")
      dirPath.createDirectory(failIfExists = false)
    })

    SortingSlave.slaveHostTable.foreach(slaveAddress => {
      // create temp file
      val file = new File(s"./temp/${SortingSlave.myAddress}/${slaveAddress}/tmp${util.getRandomTempKey}")
      if (!file.exists()) {
        logger.info(s"create: ${file.getName}")
        file.createNewFile()
        logger.info("created")
      }

      // create buffered writer
      val bw = new BufferedWriter(new FileWriter(file))
      writers = writers :+ bw
    })

    writers.foreach(w => w.close())
    logger.info("partitioning end")
  }

  def getDataFilePath(): List[String] = {
    val filePaths = SortingSlave.inputDirs
      .filter(dir => {
        SortingSlave.inputDirs.indexOf(dir) % SortingSlave.slaveHostTable.length == SortingSlave.myIndex
      })
      .flatMap(dir => {
        val d = new File(dir)
        d.listFiles()
      })
      .map(file => {
        file.getPath
      })

    logger.info(s"data files ${filePaths.toString}")
    filePaths
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
      val keys = (1 to 10000).map(_ => util.getKeyFromLine(lineNumberReader.readLine())).toSeq
      val response = GetSamplingDataReply(keys = keys)
      Future.successful(response)
    }

    override def sendPartitionTable(request: SendPartitionTableRequest): Future[SendPartitionTableReply] = {
      logger.info(s"sendPartitionTable ${request.partitionTable.toString}")
      SortingSlave.partitionTable = request.partitionTable
      Future.successful(SendPartitionTableReply())
    }

    override def sendPartitionStart(request: SendPartitionStartRequest): Future[SendPartitionStartReply] = {
      logger.info(s"sendPartitionStart")
      getDataFilePath.foreach(path => sortAndParition(path))
      Future.successful(SendPartitionStartReply())
    }

    override def sendShufflingStart(request: SendShufflingStartRequest): Future[SendShufflingStartReply] = ???

    override def sendMergeStart(request: SendMergeStartRequest): Future[SendMergeStartReply] = ???
  }

}
