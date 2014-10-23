package com.tc.mapreduce.impl;

import com.tc.mapreduce.MapReduce;
import com.tc.mapreduce.Mapper;
import com.tc.mapreduce.Reducer;
import com.tc.mapreduce.task.Runner;
import com.tc.mapreduce.util.Logger;
import com.tc.mapreduce.util.PropertiesManager;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TPEMapReduceImpl<R, M> implements MapReduce<R, M> {

   private final ExecutorService executor;
   private final BlockingQueue<Runnable> workQueue;
   private final List<Mapper<M>> mapperList;
   private final Reducer<R, Mapper<M>, M> reducer;

   public TPEMapReduceImpl(Reducer<R, Mapper<M>, M> reducer) {
      this.mapperList = new LinkedList<Mapper<M>>();
      this.reducer = reducer;
      this.workQueue = new LinkedBlockingQueue<Runnable>();

      int poolSize;
      if (PropertiesManager.getInstance().getProperty("threadpool.max").equals("true")) {
         poolSize = Runtime.getRuntime().availableProcessors();
      }
      else {
         poolSize = new Integer(PropertiesManager.getInstance().getProperty("threadpool.size"));
      }

      Logger.log("TPEMAPREDUCEIMPL", "Creating executor with thread pool size " + poolSize);
      this.executor = new ThreadPoolExecutor(poolSize, poolSize, Long.MAX_VALUE, TimeUnit.SECONDS, workQueue);

   }

   public boolean addMapper(Mapper<M> mapper) {
      if (executor.isTerminated()) {
         Logger.log("TPEMAPREDUCEIMPL", "Executor task already terminated. Cannot add mapper.");
         return false;
      }

      Runner<M> runner = new Runner<M>(mapper);
      Logger.log("TPEMAPREDUCEIMPL", "Adding mapper " + mapper.getId());
      mapperList.add(mapper);

      Logger.log("TPEMAPREDUCEIMPL", "Executing runner");
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

      Logger.log("TPEMAPREDUCEIMPL", " Performing reduce operation on " + mapperList.size() + " mappers");
      R result = reducer.reduce(mapperList);

      return result;
   }

}
