package com.tc.mapreduce.util;

public final class Logger {

   public static void log(String source, String msg) {
      if (PropertiesManager.getInstance().isLoggingEnabled()) {
         StringBuilder sb = new StringBuilder();
         sb.append("[");
         sb.append(source);
         sb.append("] INFO - ");
         sb.append(msg);

         System.out.println(sb.toString());
      }
   }

   public static void error(String source, String msg) {
      if (PropertiesManager.getInstance().isLoggingEnabled()) {
         StringBuilder sb = new StringBuilder();
         sb.append("[");
         sb.append(source);
         sb.append("] ERROR - ");
         sb.append(msg);

         System.err.println(sb.toString());
      }
   }

}
