package com.tc.mapreduce.impl;

import com.tc.mapreduce.MapReduce;
import com.tc.mapreduce.Mapper;
import com.tc.mapreduce.Reducer;
import com.tc.mapreduce.task.Runner;
import com.tc.mapreduce.util.Logger;
import com.tc.mapreduce.util.PropertiesManager;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DefaultMapReduceImpl<R, M> implements MapReduce<R, M> {

   private final ExecutorService executor;
   private final List<Mapper<M>> mapperList;
   private final Reducer<R, Mapper<M>, M> reducer;

   public DefaultMapReduceImpl(Reducer<R, Mapper<M>, M> reducer) {
      this.mapperList = new LinkedList<Mapper<M>>();
      this.reducer = reducer;

      int nThreads;
      if (PropertiesManager.getInstance().getProperty("threadpool.max").equals("true")) {
         nThreads = Runtime.getRuntime().availableProcessors();
      }
      else {
         nThreads = new Integer(PropertiesManager.getInstance().getProperty("threadpool.size"));
      }
      Logger.log("MAPREDUCEIMPL", "Creating executor with thread pool size " + nThreads);
      this.executor = Executors.newFixedThreadPool(nThreads);
   }

   public boolean addMapper(Mapper<M> mapper) {
      if (executor.isTerminated()) {
         Logger.log("MAPREDUCEIMPL", "Executor task already terminated. Cannot add mapper.");
         return false;
      }

      Runner<M> runner = new Runner<M>(mapper);
      Logger.log("MAPREDUCEIMPL", "Adding mapper " + mapper.getId());
      mapperList.add(mapper);

      Logger.log("MAPREDUCEIMPL", "Executing runner");
      executor.execute(runner);

      return true;
   }

   public R getResult() {
      try {
         executor.shutdown();
         executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
      }
      catch (InterruptedException e) {
         Logger.error(this.getClass().getName().toUpperCase(), e.getStackTrace().toString());
      }

      Logger.log("MAPREDUCEIMPL", " Performing reduce operation on " + mapperList.size() + " mappers");
      R result = reducer.reduce(mapperList);

      return result;
   }

}
