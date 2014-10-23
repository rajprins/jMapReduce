package com.tc.mapreduce.example;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import com.tc.mapreduce.MapReduce;
import com.tc.mapreduce.Mapper;
import com.tc.mapreduce.Reducer;
import com.tc.mapreduce.impl.DefaultMapReduceImpl;
import com.tc.mapreduce.util.Logger;

public class RandomNumbersDemo {

   private static MapReduce<Integer, Integer> mapReduce;
   private static final int demoNumber = 1000;

   public static void main(String[] args) {
      System.out.println("[DEMO] Starting map reduce process for calculating " + demoNumber + " * " + demoNumber + " random numbers.");
      long startTime = System.currentTimeMillis();

      // ------------------------------------------------------------
      // Start with setting up a reducer
      // ------------------------------------------------------------
      AtomicReference<Reducer<Integer, Mapper<Integer>, Integer>> reducer = new AtomicReference<Reducer<Integer, Mapper<Integer>, Integer>>(
            new Reducer<Integer, Mapper<Integer>, Integer>() {

               private static final long serialVersionUID = 1L;

               public Integer reduce(List<Mapper<Integer>> mappers) {
                  int result = 0;
                  for (int key = 0; key < mappers.size(); key++) {
                     result += mappers.get(key).getResult();
                  }
                  return result;
               }
            });

      // ------------------------------------------------------------
      // Next, set up the mapreduce implementation of choice
      // ------------------------------------------------------------
      mapReduce = new DefaultMapReduceImpl<Integer, Integer>(reducer.get());
      Mapper<Integer> mapper;

      // ------------------------------------------------------------
      // Finally, create x number of mappers (where x is defined by
      // 'demoNumber')
      // ------------------------------------------------------------
      for (int i = 0; i < demoNumber; i++) {
         // new mapper
         mapper = new Mapper<Integer>() {
            static final long serialVersionUID = 1L;

            // ID of the mapper
            Long id = System.nanoTime();
            int result = 0;

            // Add the computing logic in the map method
            public void map() {
               Logger.log("DEMO", "Mapper " + id + ": calculating sum of 1000 random numbers.");

               // Put your number crunching code here...
               for (int j = 0; j < demoNumber; j++) {
                  Random random = new Random();
                  random.setSeed(System.currentTimeMillis());
                  result += random.nextInt(demoNumber);
               }
            }

            public Integer getResult() {
               return result;
            }

            public Long getId() {
               if (this.id == null) {
                  return (long) 0;
               }
               return this.id;
            }

         };

         // Add the mapper to the chosen mapreduce implementation
         mapReduce.addMapper(mapper);
      }

      // ------------------------------------------------------------
      // Show some statistics
      // ------------------------------------------------------------
      long endTime = System.currentTimeMillis();
      System.out.println("[DEMO] Result: " + mapReduce.getResult());
      System.out.println("[DEMO] Elapsed time: " + (endTime - startTime) + " ms.");

   }

}
