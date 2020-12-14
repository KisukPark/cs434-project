## How to run
```sh
# sbt's console
sbt
runMain sorting.SortingMaster 3
runMain sorting.SortingSlave 192.168.29.140:7001 -I ./input/data1 ./input/data2 ./input/data3 ./input/data4 ./input/data5 -O ./output
```

## Logging
```scala
val logger = Logger(classOf[SortingMaster])
logger.debug(s"hi $val")
// "prefix hi val"
```

## Tips
```scala
// 내 ip 주소 찾기
InetAddress.getLocalHost.getHostAddress

// gRPC server
val server: Server = ServerBuilder
  .forPort("7001")
  .addService(GreeterGrpc.bindService(new GreeterImpl, executionContext))
  .build.start

private class GreeterImpl extends GreeterGrpc.Greeter {
  override def sayHello(req: HelloRequest) = {
    logger.info(s"sayHello is called : $req.name")
    val reply = HelloReply(message = "Hello " + req.name)
    Future.successful(reply)
  }
}

// gRPC client
val channel: ManagedChannel = ManagedChannelBuilder
  .forAddress("192.1698.10.10", "7001")
  .usePlaintext()
  .build
val blockingStub: GreeterBlockingStub = GreeterGrpc.blockingStub(channel)
val response = blockingStub.sayHello(request)
```

## Syntaxs
```scala
seqvar.foreach(a => { })
seqvar.map(a => { })
seqvar.flatMap(a => { })
(1 to 10).foreach(a => { })

for (i <- 1 to 10) { }

append 1 item	oldSeq :+ e
append multiple items	oldSeq ++ newSeq
prepend 1 item	e +: oldSeq
prepend multiple items	newSeq ++: oldSeq
```
