package com.tc.mapreduce.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public final class PropertiesManager {

   private final static Properties properties = new Properties();
   private boolean propsLoaded = false;
   private static PropertiesManager instance;

   private PropertiesManager() {}

   public static PropertiesManager getInstance() {
      if (instance == null) {
         instance = new PropertiesManager();
      }

      return instance;
   }

   public Properties getProperties() {
      if (!propsLoaded) {
         loadProperties();
      }

      return properties;
   }

   public String getProperty(String prop) {
      return getProperties().getProperty(prop);
   }

   public boolean isLoggingEnabled() {
      return getProperties().getProperty("logging.enabled").equals("true");
   }

   private void loadProperties() {
      try {
         System.out.println("[PropertiesManager] Loading properties");
         InputStream in = getClass().getResourceAsStream("/mapreduce.properties");
         properties.load(in);
         propsLoaded = true;
      }
      catch (IOException oops) {
         System.err.println(oops.getLocalizedMessage());
      }
   }

}
