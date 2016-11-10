package com.bfd.spark.schedule

import java.util.Timer
import java.util.TimerTask
import java.util.Calendar
import java.util.Date
/**
 * 用于切换topic的调度
 */
class TimeSchedule(val timer: Timer) {
  def addFixedTask(hour: Int, minute: Int, second: Int, task: TimerTask) {
    val calendar = Calendar.getInstance();
    calendar.set(Calendar.HOUR_OF_DAY, hour);
    calendar.set(Calendar.MINUTE, minute);
    calendar.set(Calendar.SECOND, second);
    var date = calendar.getTime();
    if (date.before(new Date())) {
      date = addDay(date, 1);
    }
    timer.schedule(task, date, 1000 * 60 * 60 * 24);

  }

  def scheduleAtFixRate(task: TimerTask, first: Date) {
    timer.scheduleAtFixedRate(task, first, 1000 * 60 * 1)
  }
  private def addDay(date: Date, num: Int) = {
    val startDT = Calendar.getInstance();
    startDT.setTime(date);
    startDT.add(Calendar.DAY_OF_MONTH, num);
    startDT.getTime();
  }
}