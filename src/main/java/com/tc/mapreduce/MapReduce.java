package com.tc.mapreduce;

public interface MapReduce<R, M> {

   boolean addMapper(Mapper<M> mapper);

   R getResult();

}
