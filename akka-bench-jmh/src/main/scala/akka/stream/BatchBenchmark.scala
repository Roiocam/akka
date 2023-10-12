/*
 * Copyright (C) 2014-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import akka.Done
import akka.NotUsed
import akka.actor.ActorSystem
import akka.remote.artery.BenchTestSourceSameElement
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.RunnableGraph
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import org.openjdk.jmh.annotations._

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration._

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.SECONDS)
@BenchmarkMode(Array(Mode.Throughput))
class BatchBenchmark {
  implicit val system: ActorSystem = ActorSystem("BatchBenchmark")
  val ex = Executors.newSingleThreadExecutor()
  implicit val executor: ExecutionContextExecutor = ExecutionContext.fromExecutor(ex)

  val NumberOfElements = 100000
  val SmallWindows = 100
  val LargeWindows = NumberOfElements << 1
  val BufferSize = 100
  val Timeout = 100.millis
  @Param(
    Array(
      "groupedWeightedWithin",
      "batchWeighted",
      "groupedWeightedWithin_withLargeWindows",
      "batchWeighted_withLargeWindows",
      "groupedWeightedWithin_withBuffer",
      "batchWeighted_withBuffer"))
  var BatchWith = ""

  var graph: RunnableGraph[Future[Done]] = _

  def createSource(count: Int): Graph[SourceShape[java.lang.Integer], NotUsed] =
    new BenchTestSourceSameElement(count, 1)

  @Setup
  def setup(): Unit = {
    val source = createSource(NumberOfElements)
    val slowSink: Sink[Any, Future[Done]] = Sink.foreachAsync(1)(processElement)
    graph = BatchWith match {
      case "groupedWeightedWithin" =>
        Source.fromGraph(source).groupedWeightedWithin(SmallWindows, Timeout)(_ => 1).toMat(slowSink)(Keep.right)
      case "batchWeighted" =>
        Source
          .fromGraph(source)
          .batchWeighted(max = SmallWindows, _ => 1, seed = i => i)(aggregate = _ + _)
          .toMat(slowSink)(Keep.right)
      case "groupedWeightedWithin_withLargeWindows" =>
        Source.fromGraph(source).groupedWeightedWithin(LargeWindows, Timeout)(_ => 1).toMat(slowSink)(Keep.right)
      case "batchWeighted_withLargeWindows" =>
        Source
          .fromGraph(source)
          .batchWeighted(max = LargeWindows, _ => 1, seed = i => i)(aggregate = _ + _)
          .toMat(slowSink)(Keep.right)
      case "groupedWeightedWithin_withBuffer" =>
        Source
          .fromGraph(source)
          .groupedWeightedWithin(LargeWindows, Timeout)(_ => 1)
          .buffer(BufferSize, OverflowStrategy.backpressure)
          .toMat(slowSink)(Keep.right)
      case "batchWeighted_withBuffer" =>
        Source
          .fromGraph(source)
          .batchWeighted(max = LargeWindows, _ => 1, seed = i => i)(aggregate = _ + _)
          .buffer(BufferSize, OverflowStrategy.backpressure)
          .toMat(slowSink)(Keep.right)
    }

    // eager init of materializer
    SystemMaterializer(system).materializer
  }

  def processElement(ele: Any): Future[Unit] = {
    Future {
      Thread.sleep(10)
      ele.hashCode()
    }
  }

  @TearDown
  def shutdown(): Unit = {
    ex.shutdown()
    Await.result(system.terminate(), 5.seconds)
  }

  @Benchmark
  @OperationsPerInvocation(100000) // Note: needs to match NumberOfElements.
  def batch_100k_elements(): Unit = {
    Await.result(graph.run(), Duration.Inf)
  }
}
