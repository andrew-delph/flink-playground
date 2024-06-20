package adelph.scala

import org.apache.flink.api.common.functions.MapFunction

class IncrementMapFunctionScala extends MapFunction[Long, Long] with Serializable {
  override def map(record: Long): Long = record + 1
}