# Trouble Shooting

### 서버를 중지해도 port 가 계속 사용되는 문제
```sbt
// Add below in build.sbt
fork in run := true
```
[StackOverflow : Why do we need to add “fork in run := true” when running Spark SBT application?](https://stackoverflow.com/questions/44298847/why-do-we-need-to-add-fork-in-run-true-when-running-spark-sbt-application)
