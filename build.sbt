import scalapb.compiler.Version.{grpcJavaVersion, scalapbVersion}

name := "cs434-project"

version := "0.1"

scalaVersion := "2.13.3"

libraryDependencies ++= Seq(
  "io.grpc" % "grpc-netty" % scalapb.compiler.Version.grpcJavaVersion,
  "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion
)

PB.protoSources in Compile += (baseDirectory in LocalRootProject).value / "src/main/protobuf"
PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)

fork in run := true
