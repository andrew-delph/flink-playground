
package adelph.scala

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.{PrintSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction

@SerialVersionUID(1L)
class StreamingJobScala(source: SourceFunction[Long], sink: SinkFunction[Long]) {

  def execute(): Unit = {
    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    val longStream: DataStream[Long] = env.addSource(source)
      .returns(TypeInformation.of(classOf[Long]))

    longStream
      .map(new IncrementMapFunctionScala()).returns(TypeInformation.of(classOf[Long]))
      .addSink(sink)

    println("hi")
    env.execute()
  }

}

object StreamingJobScala {
  def main(args: Array[String]): Unit = {
    val job = new StreamingJobScala(new LongCounterSourceScala, new PrintSinkFunction[Long]())
    job.execute()
  }
}
