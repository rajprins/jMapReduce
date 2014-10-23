package com.tc.mapreduce;

import java.io.Serializable;

public interface Mapper<T> extends Serializable {

   void map();

   T getResult();

   Long getId();

}
