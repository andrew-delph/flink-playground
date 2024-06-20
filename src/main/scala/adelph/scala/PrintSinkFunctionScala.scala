package adelph.scala

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.api.java.tuple.Tuple2

class PrintSinkFunctionScala extends SinkFunction[Tuple2[String, Integer]] {
  override def invoke(value: Tuple2[String, Integer], context: SinkFunction.Context): Unit = {
    if (value.f1 > 2) {
      println(s"Word: ${value.f0}, Count: ${value.f1}")
    }
  }
}