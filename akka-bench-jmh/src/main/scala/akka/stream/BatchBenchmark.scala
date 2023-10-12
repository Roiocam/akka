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

import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration._

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.SECONDS)
@BenchmarkMode(Array(Mode.Throughput))
class BatchBenchmark {
  implicit val system: ActorSystem = ActorSystem("BatchBenchmark")

  val NumberOfElements = 100000
  @Param(Array("groupedWeightedWithin", "batchWeighted"))
  var BatchWith = ""

  var graph: RunnableGraph[Future[Done]] = _

  def createSource(count: Int): Graph[SourceShape[java.lang.Integer], NotUsed] =
    new BenchTestSourceSameElement(count, 1)

  @Setup
  def setup(): Unit = {
    val source = createSource(NumberOfElements)
    graph = BatchWith match {
      case "groupedWeightedWithin" =>
        Source.fromGraph(source).groupedWeightedWithin(100, 100.millis)(_ => 1).toMat(Sink.ignore)(Keep.right)
      case "batchWeighted" =>
        Source
          .fromGraph(source)
          .batchWeighted(max = 100, _ => 1, seed = i => i)(aggregate = _ + _)
          .toMat(Sink.ignore)(Keep.right)
    }

    // eager init of materializer
    SystemMaterializer(system).materializer
  }

  @TearDown
  def shutdown(): Unit = {
    Await.result(system.terminate(), 5.seconds)
  }

  @Benchmark
  @OperationsPerInvocation(100000) // Note: needs to match NumberOfElements.
  def batch_100k_elements(): Unit = {
    Await.result(graph.run(), Duration.Inf)
  }
}
