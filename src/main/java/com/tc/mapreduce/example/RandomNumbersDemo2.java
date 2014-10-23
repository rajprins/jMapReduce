package com.tc.mapreduce.example;

import com.tc.mapreduce.MapReduce;
import com.tc.mapreduce.Mapper;
import com.tc.mapreduce.Reducer;
import com.tc.mapreduce.impl.TPEMapReduceImpl;
import com.tc.mapreduce.util.Logger;

import java.util.List;
import java.util.Random;

public class RandomNumbersDemo2 {

   private static MapReduce<Integer, Integer> mapReduce;
   private static int demoNumber = 1000;

   public static void main(String[] args) {
      System.out.println("[DEMO] Starting map reduce process for calculating " + demoNumber + " * " + demoNumber + " random numbers.");
      long startTime = System.currentTimeMillis();

      // ------------------------------------------------------------
      // Start with setting up a reducer
      // ------------------------------------------------------------
      Reducer<Integer, Mapper<Integer>, Integer> reducer = new Reducer<Integer, Mapper<Integer>, Integer>() {
         static final long serialVersionUID = 1L;

         public Integer reduce(List<Mapper<Integer>> mappers) {
            int result = 0;
            for (int key = 0; key < mappers.size(); key++) {
               result += mappers.get(key).getResult();
            }
            return result;
         }
      };

      // ------------------------------------------------------------
      // Next, set up the mapreduce implementation of choice
      // ------------------------------------------------------------
      mapReduce = new TPEMapReduceImpl<Integer, Integer>(reducer);
      Mapper<Integer> mapper;

      // ------------------------------------------------------------
      // Finally, create x number of mappers (where x is defined by
      // 'demoNumber')
      // ------------------------------------------------------------
      for (int i = 0; i < demoNumber; i++) {
         mapper = new Mapper<Integer>() {
            static final long serialVersionUID = 1L;
            Long id = System.nanoTime();
            Random random = new Random();
            int result = 0;

            public void map() {
               Logger.log("DEMO", "Mapper " + id + ": calculating sum of 1000 random numbers.");
               for (int j = 0; j < demoNumber; j++) {
                  random.setSeed(System.currentTimeMillis());
                  result += random.nextInt(demoNumber);
               }
            }

            public Integer getResult() {
               return result;
            }

            public Long getId() {
               if (this.id == null) {
                  return new Long(0);
               }
               return this.id;
            }

         };
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
