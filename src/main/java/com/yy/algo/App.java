package com.yy.algo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {

        List<Double> seq = new ArrayList<>();
//        seq.add(1.0);
//        seq.add(0.0);
//        seq.add(2.0);
        Double[] arr = {1.0, 0.0, 2.0, 2.0, 0.1, 0.1, 0.2, 3.0, 2.5, 3.1, 0.2, 5.1, 1.8, 0.2, 2.3};
        for(double e:arr){
            seq.add(e);
        }
        System.out.println(interpolate(seq));
//        System.out.println(interpolate2(seq));
//        System.out.println(interpolate3(seq));
        System.out.println(Arrays.toString(seq.toArray(new Double[0])));
    }


    public static List<Double> interpolate(List<Double> seq){
        Double[] arr = seq.toArray(new Double[0]);
        List<Double> result = new ArrayList<>();
        if(seq==null){
            return result;
        }
        int left = 0;
        int right = 1;
        double start = arr[0];
        double end = arr[0];
        int step = 0;
        while(right<arr.length){
            start = arr[left];
            end = arr[right];
            step = right-left;
            if(end>=start){
                while(left<(right-1)){
                    arr[left+1] = arr[left] + (end-start)/step;
                    left++;
                }
                right++;
                left++;
            }else{
                right++;
            }
        }
        if(right==arr.length && left<(arr.length-1)){
            while(end<start && left<(arr.length-1)){
                arr[left+1] = start;
                left++;
            }
        }

        for(double e:arr){
            result.add(e);
        }

        return result;

    }

//
//    public static List<Double> linearInterpolate(List<Double> v) {
//        List<Double> result = new ArrayList<>();
//        if (v == null || v.isEmpty()) {
//            return result;
//        }
//
//        int n = v.size();
//        int i = 0;
//        while (i < n) {
//            result.add(v.get(i));
//            int j = i + 1;
//            while (j < n && v.get(j) < v.get(i)) {
//                j++;
//            }
//            if (j < n) {
//                int steps = j - i;
//                double start = v.get(i);
//                double end = v.get(j);
//                double stepSize = (end - start) / steps;
//                for (int k = 1; k < steps; k++) {
//                    double interpolatedValue = start + stepSize * k;
//                    result.add(interpolatedValue);
//                }
//            } else {
//                for (int k = i + 1; k < n; k++) {
//                    result.add(v.get(i));
//                }
//            }
//            i = j;
//        }
//
//        return result;
//    }
}
