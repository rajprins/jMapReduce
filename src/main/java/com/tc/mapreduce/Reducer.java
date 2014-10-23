package com.tc.mapreduce;

import java.io.Serializable;
import java.util.List;

public interface Reducer<T, V extends Mapper<M>, M> extends Serializable {

   T reduce(List<V> mappers);

}
