package org.gen

import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.ReducingStateDescriptor
import org.apache.flink.api.common.typeutils.base.LongSerializer
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton

//TODO
class CustomCountTrigger[W <:Window] extends Trigger[List[Any], W]{


  private val serialVersionUID = 1L

  private val maxCount = 0L

  private val stateDesc = new ReducingStateDescriptor[Long]("count", new Sum(), LongSerializer.INSTANCE.asInstanceOf[TypeSerializerSingleton[Long]])


  @throws[Exception] override def onElement(element: List[Any], timestamp: Long, window: W, ctx: Trigger.TriggerContext): TriggerResult = {
    val count = ctx.getPartitionedState(stateDesc)
    count.add(1L)
    if (count.get >= element.length) {
      count.clear()
      return TriggerResult.FIRE
    }
    TriggerResult.CONTINUE
  }

  override def onEventTime(time: Long, window: W, ctx: Trigger.TriggerContext) = TriggerResult.CONTINUE

  @throws[Exception] override def onProcessingTime(time: Long, window: W, ctx: Trigger.TriggerContext) = TriggerResult.CONTINUE

  @throws[Exception] override def clear(window: W, ctx: Trigger.TriggerContext): Unit = {
    ctx.getPartitionedState(stateDesc).clear()
  }

  override def canMerge = true

  @throws[Exception] override def onMerge(window: W, ctx: Trigger.OnMergeContext): Unit = {
    ctx.mergePartitionedState(stateDesc)
  }

  override def toString: String = "CountTrigger(" + maxCount + ")"

  private class Sum extends ReduceFunction[Long] {
    @throws[Exception] override def reduce(value1: Long, value2: Long): Long = value1 + value2
  }


}

object CustomCountTrigger {
  def of[W <: Window]() = new CustomCountTrigger[W]
}