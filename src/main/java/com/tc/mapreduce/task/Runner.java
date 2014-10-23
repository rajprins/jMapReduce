package com.tc.mapreduce.task;

import com.tc.mapreduce.Mapper;
import com.tc.mapreduce.util.Logger;

public class Runner<M> implements Runnable {

   private final Mapper<M> mapper;

   public Runner(Mapper<M> mapper) {
      Logger.log("RUNNER", "New runner created for mapper " + mapper.getId());
      this.mapper = mapper;
   }

   public void run() {
      // Run the computing tasks
      Logger.log("RUNNER", "Running mapper " + mapper.getId() + " map operation");
      mapper.map();
   }
}
