package com.tc.mapreduce.example;

import com.tc.mapreduce.MapReduce;
import com.tc.mapreduce.Mapper;
import com.tc.mapreduce.Reducer;
import com.tc.mapreduce.impl.DefaultMapReduceImpl;

import java.util.List;

public class HelloWorldMapReduceDemo {

   private static MapReduce<String, String> mapReduce;

   public static void main(String[] args) {

      // ------------------------------------------------------------
      // Step 1: set up a reducer
      // ------------------------------------------------------------
      Reducer<String, Mapper<String>, String> reducer = new Reducer<String, Mapper<String>, String>() {
         static final long serialVersionUID = 1L;

         public String reduce(List<Mapper<String>> mappers) {
            String result = "";
            for (int key = 0; key < mappers.size(); key++) {
               result += mappers.get(key).getResult();
            }
            return result;
         }
      };

      // ------------------------------------------------------------
      // Step 2: set up the mapreduce implementation of choice
      // ------------------------------------------------------------
      mapReduce = new DefaultMapReduceImpl<String, String>(reducer);
      Mapper<String> mapper;

      // ------------------------------------------------------------
      // Step 3: create x number of mappers, in this example just 2
      // ------------------------------------------------------------
      for (int i = 0; i < 2; i++) {
         final int chunk = i;

         // Create new mapper
         mapper = new Mapper<String>() {

            private static final long serialVersionUID = 1L;

            // ID of the mapper
            Long id = System.nanoTime();

            String result = "";

            public Long getId() {
               if (this.id == null) {
                  return (long) 0;
               }
               return this.id;
            }

            // The map method contains actual business logic/number crunching
            // routines
            public void map() {
               if (chunk % 2 == 0)
                  result = "Hello";
               else
                  result = " World";
            }

            public String getResult() {
               return result;
            }
         };

         // Add the mapper to the chosen mapreduce implementation
         mapReduce.addMapper(mapper);
      }

      // ------------------------------------------------------------
      // Step 4: Show results
      // ------------------------------------------------------------
      System.out.println(mapReduce.getResult());
   }
}
