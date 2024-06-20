package adelph.scala

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.util.Random

class LongCounterSourceScala extends RichParallelSourceFunction[Long] {

  @volatile private var cancelled = false
  private var counter: Long = 0L

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (!cancelled) {
      counter += 1
      if (counter > 10) {
        counter = 0
      }
      ctx.getCheckpointLock.synchronized {
        ctx.collect(counter)
      }
    }
  }

  override def cancel(): Unit = {
    cancelled = true
  }
}
