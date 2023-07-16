package com.yy.algo;


import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@Description(name = "interpolate_seq",
        value = "_FUNC_(time, value) - Interpolates the sequence of values",
        extended = "Example:\n" +
                "  SELECT station_name, interpolate_seq(time, value) AS map_col FROM my_table GROUP BY station_name")
public class InterpolateSeqUDAF extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 2) {
            throw new UDFArgumentLengthException("The interpolate_seq function requires exactly two arguments: time and value");
        }

        if (!parameters[0].equals(TypeInfoFactory.longTypeInfo)) {
            throw new UDFArgumentTypeException(0, "The first argument (time) must be of type long");
        }

        if (!parameters[1].equals(TypeInfoFactory.doubleTypeInfo)) {
            throw new UDFArgumentTypeException(1, "The second argument (value) must be of type double");
        }

        return new InterpolateSeqUDAFEvaluator();
    }

    public static class InterpolateSeqUDAFEvaluator extends GenericUDAFEvaluator {

        private LongObjectInspector timeOI;
        private DoubleObjectInspector valueOI;

        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] parameters) throws HiveException {

            // Input inspectors
            timeOI = (LongObjectInspector) parameters[0];
            valueOI = (DoubleObjectInspector) parameters[1];

            // Output inspector
            return ObjectInspectorFactory.getStandardMapObjectInspector(
                    PrimitiveObjectInspectorFactory.javaLongObjectInspector,
                    PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            // Create and return a new aggregation buffer
            return new InterpolateSeqAggregationBuffer();
        }

        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            // Reset the aggregation buffer
            ((InterpolateSeqAggregationBuffer) aggregationBuffer).reset();
        }

        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] parameters) throws HiveException {
            // Perform the iteration logic
            if (parameters[0] != null && parameters[1] != null) {
                Long time = (Long) timeOI.getPrimitiveJavaObject(parameters[0]);
                Double value = (Double) valueOI.getPrimitiveJavaObject(parameters[1]);
                ((InterpolateSeqAggregationBuffer) aggregationBuffer).iterate(time, value);
            }
        }

        @Override
        public List<Double> terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            // Return the partial result
            return ((InterpolateSeqAggregationBuffer) aggregationBuffer).getResult();
        }

        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object partialResult) throws HiveException {
            // Perform the merge logic
            if (partialResult != null) {
                ((InterpolateSeqAggregationBuffer) aggregationBuffer).merge((Map<Long, Double>) partialResult);
            }
        }

        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            // Return the final result
            return linearInterpolate(((InterpolateSeqAggregationBuffer) aggregationBuffer).getResult());
        }

        public static List<Double> linearInterpolate(List<Double> v) {
            List<Double> result = new ArrayList<>();
            if (v == null || v.isEmpty()) {
                return result;
            }

            int n = v.size();
            int i = 0;
            while (i < n) {
                result.add(v.get(i));
                int j = i + 1;
                while (j < n && v.get(j) < v.get(i)) {
                    j++;
                }
                if (j < n) {
                    int steps = j - i;
                    double start = v.get(i);
                    double end = v.get(j);
                    double stepSize = (end - start) / steps;
                    for (int k = 1; k < steps; k++) {
                        double interpolatedValue = start + stepSize * k;
                        result.add(interpolatedValue);
                    }
                } else {
                    for (int k = i + 1; k < n; k++) {
                        result.add(v.get(i));
                    }
                }
                i = j;
            }

            return result;
        }

        public static class InterpolateSeqAggregationBuffer implements AggregationBuffer {
            private TreeMap<Long, Double> sequenceMap;

            public InterpolateSeqAggregationBuffer() {
                reset();
            }

            public void reset() {
                sequenceMap = new TreeMap<>();
            }

            public void iterate(Long time, Double value) {
                sequenceMap.put(time, value);
            }

            public void merge(Map<Long, Double> otherMap) {
                sequenceMap.putAll(otherMap);
            }

            public List<Double> getResult() {
                List<Double> values = new ArrayList<>(sequenceMap.values());
                return linearInterpolate(values);
            }
        }
    }
}
