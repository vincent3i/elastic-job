package com.dangdang.ddframe.test;

import com.dangdang.ddframe.concurrent.ForkBlur;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.LocaleUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.RecursiveTask;

/**
 * Created by zruan on 2015/12/23.
 */
public class SegmentTest {

    @Test
    public void testStringUtils() {
        System.out.println(RandomStringUtils.random(20));
        System.out.println(RandomStringUtils.random(20, true, true));
        System.out.println(RandomStringUtils.randomAlphabetic(20));
        System.out.println(RandomStringUtils.randomAscii(20));
        System.out.println(RandomStringUtils.randomNumeric(20));


        System.out.println(StringEscapeUtils.escapeJson("{\"app\":\"dd\"}"));
        System.out.println(LocaleUtils.availableLocaleList());
        int n = 8;
        n *= 8;
        System.out.println(n);
        System.out.println(1 / 9);
    }

    @Test
    public void test002() {
        int number = 10;
        //原始数二进制
        this.printNumber(number);

        //左移一位
        number = number << 1;
        this.printNumber(number);

        number = 10;
        //右移一位
        number = number >> 1;
        this.printNumber(number);

        number = 10;
        this.printNumber(number);
        //无符号右移一位
        number = number >>> 1;
        this.printNumber(number);
    }

    private void printNumber(int number) {
        System.out.println("Original ==> [" + number + "], hex ==> [" + Integer.toBinaryString(number) + "]");
    }

    @Test
    public void testDigest() {
        System.out.println(DigestUtils.sha512Hex("123456789"));
        System.out.println(DigestUtils.sha512Hex(DigestUtils.sha512Hex("123456789")));
        System.out.println(DigestUtils.md5Hex("Oracle123"));
    }

    // 归并排序的实现
    @Test
    public void testMergeSort() {

        int[] nums = {2, 7, 8, 3, 1, 6, 9, 0, 5, 4};

        SortTask.sort(nums, 0, nums.length - 1);
        System.out.println(Arrays.toString(nums));
    }

    @Test
    public void testForkJoinSum() {
        // source image pixels are in src
        // destination image pixels are in dst
        int len = (int) Math.pow(10, 2);
        int[] src = new int[len], dst = new int[len];
        for (int i = 0; i < len; i++) {
            src[i] = RandomUtils.nextInt(1, 10000);
        }

        ForkBlur fb = new ForkBlur(src, 0, src.length, dst);
        ForkJoinPool pool = new ForkJoinPool();
        pool.invoke(fb);
        for (int i = 0; i < src.length; i++) {
            System.out.println(src[i]);
        }
        for (int i = 0; i < dst.length; i++) {
            System.out.println(dst[i]);
        }
    }

    /**
     * Not passed
     */
    @Test
    public void testForkJoinMergeSort() {
        // source image pixels are in src
        // destination image pixels are in dst
        int len = (int) Math.pow(10, 2);
        int[] src = new int[len];
        for (int i = 0; i < len; i++) {
            src[i] = RandomUtils.nextInt(1, 1000);
        }

        SortTask st = new SortTask(src, 0, src.length - 1);
        ForkJoinPool pool = new ForkJoinPool();
        pool.invoke(st);
        System.out.println(ReflectionToStringBuilder.toString(src));
    }

    @Test
    public void testSum() {
        ForkJoinPool fjPool = new ForkJoinPool();
        int[] array = new int[100000];
        for (int i = 0; i < array.length; i++) {
            array[i] = i;
        }
        long sum = fjPool.invoke(new Sum(array, 0, array.length, "all"));
        System.out.println(sum);
    }

    @Test
    public void testSumOfSquares() {
        ForkJoinPool fjPool = new ForkJoinPool();
        int len = (int) Math.pow(10, 2);
        double[] src = new double[len];
        for (int i = 0; i < len; i++) {
            src[i] = RandomUtils.nextDouble(0.0, 10000.0);
        }
        System.out.println(sumOfSquares(fjPool, src));
    }

    double sumOfSquares(ForkJoinPool pool, double[] array) {
        int n = array.length;
        Applyer a = new Applyer(array, 0, n, null);
        pool.invoke(a);
        return a.result;
    }

    @Test
    public void testCountTask() {
        long beginTime = System.nanoTime();
        System.out.println("The sum from 1 to 1000 is " + sum(1, 1000));
        System.out.println("Time consumed(nano second) By recursive algorithm : " + (System.nanoTime() - beginTime));

        beginTime = System.nanoTime();
        System.out.println("The sum from 1 to 1000000000 is " + sum1(1, 1000000000));
        System.out.println("Time consumed(nano second) By loop algorithm : " + (System.nanoTime() - beginTime));

        ForkJoinPool forkJoinPool = new ForkJoinPool();
        CountTask task = new CountTask(1, 1000000000);
        beginTime = System.nanoTime();
        Future<Long> result = forkJoinPool.submit(task);
        try {
            System.out.println("The sum from 1 to 1000000000 is " + result.get());
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("Time consumed(nano second) By ForkJoin algorithm : " + (System.nanoTime() - beginTime));
    }

    public static long sum1(long start, long end) {
        long s = 0l;

        for (long i = start; i <= end; i++) {
            s += i;
        }

        return s;
    }

    public static long sum(long start, long end) {
        if (end > start) {
            return end + sum(start, end - 1);
        } else {
            return start;
        }
    }
}

class Sum extends RecursiveTask<Long> {
    static final int SEQUENTIAL_THRESHOLD = 5000;

    int low;
    int high;
    int[] array;
    String flag;

    Sum(int[] arr, int lo, int hi, String flag) {
        array = arr;
        low = lo;
        high = hi;
        this.flag = flag;
    }

    protected Long compute() {
        if (high - low <= SEQUENTIAL_THRESHOLD) {
            long sum = 0;
            for (int i = low; i < high; ++i)
                sum += array[i];
            return sum;
        }

        int mid = low + (high - low) / 2;
        System.out.println("flag ==>" + this.flag + ", low ==>" + low + ", mid ==>" + mid + ", high==>" + high);
        Sum left = new Sum(array, low, mid, "left");
        Sum right = new Sum(array, mid, high, "right");
        left.fork();
        long rightAns = right.compute();
        long leftAns = left.join();
        return leftAns + rightAns;
    }
}

class SortTask extends RecursiveAction {
    static final int THRESHOLD = 50;
    final int[] array;
    final int lo;
    final int hi;

    SortTask(int[] array, int lo, int hi) {
        this.array = array;
        this.lo = lo;
        this.hi = hi;
    }

    protected void compute() {
        if (hi - lo < THRESHOLD) {
            System.out.println("------------------------");
            sort(array, lo, hi);
        } else {
            int mid = (lo + hi) >>> 1;
            invokeAll(new SortTask(array, lo, mid),
                    new SortTask(array, mid, hi));
            merge(array, lo, mid, hi);
        }
    }

    /**
     * 归并排序
     * 简介:将两个（或两个以上）有序表合并成一个新的有序表 即把待排序序列分为若干个子序列，每个子序列是有序的。然后再把有序子序列合并为整体有序序列
     * 时间复杂度为O(nlogn)
     * 稳定排序方式
     *
     * @param nums 待排序数组
     * @return 输出有序数组
     */
    public static int[] sort(int[] nums, int low, int high) {
        int mid = (low + high) / 2;
        if (low < high) {
            // 左边
            sort(nums, low, mid);
            // 右边
            sort(nums, mid + 1, high);
            // 左右归并
            merge(nums, low, mid, high);
        }
        return nums;
    }

    public static void merge(int[] nums, int low, int mid, int high) {
        int[] temp = new int[high - low + 1];
        int i = low;// 左指针
        int j = mid + 1;// 右指针
        int k = 0;

        // 把较小的数先移到新数组中
        while (i <= mid && j <= high) {
            System.out.println("==>" + temp.length + ", " + i + ", " + j + ", number len ==>" + nums.length);
            if (nums[i] < nums[j]) {
                temp[k++] = nums[i++];
            } else {
                temp[k++] = nums[j++];
            }
        }

        // 把左边剩余的数移入数组
        while (i <= mid) {
            temp[k++] = nums[i++];
        }

        // 把右边边剩余的数移入数组
        while (j <= high) {
            temp[k++] = nums[j++];
        }

        // 把新数组中的数覆盖nums数组
        for (int k2 = 0; k2 < temp.length; k2++) {
            nums[k2 + low] = temp[k2];
        }
    }

}


class IncrementTask extends RecursiveAction {
    static final int THRESHOLD = 5000;
    final long[] array;
    final int lo;
    final int hi;

    IncrementTask(long[] array, int lo, int hi) {
        this.array = array;
        this.lo = lo;
        this.hi = hi;
    }

    protected void compute() {
        if (hi - lo < THRESHOLD) {
            for (int i = lo; i < hi; ++i)
                array[i]++;
        } else {
            int mid = (lo + hi) >>> 1;
            invokeAll(new IncrementTask(array, lo, mid),
                    new IncrementTask(array, mid, hi));
        }
    }
}

class Applyer extends RecursiveAction {
    final double[] array;
    final int lo, hi;
    double result;
    Applyer next; // keeps track of right-hand-side tasks

    Applyer(double[] array, int lo, int hi, Applyer next) {
        this.array = array;
        this.lo = lo;
        this.hi = hi;
        this.next = next;
    }

    double atLeaf(int l, int h) {
        double sum = 0;
        for (int i = l; i < h; ++i) // perform leftmost base step
            sum += array[i] * array[i];
        return sum;
    }

    protected void compute() {
        int l = lo;
        int h = hi;
        Applyer right = null;
        while (h - l > 1 && getSurplusQueuedTaskCount() <= 3) {
            int mid = (l + h) >>> 1;
            right = new Applyer(array, mid, h, right);
            right.fork();
            h = mid;
        }
        double sum = atLeaf(l, h);
        while (right != null) {
            if (right.tryUnfork()) // directly calculate if not stolen
                sum += right.atLeaf(right.lo, right.hi);
            else {
                right.join();
                sum += right.result;
            }
            right = right.next;
        }
        result = sum;
    }
}

class Fibonacci extends RecursiveTask<Integer> {
    final int n;

    Fibonacci(int n) {
        this.n = n;
    }

    protected Integer compute() {
        if (n <= 1)
            return n;
        Fibonacci f1 = new Fibonacci(n - 1);
        f1.fork();
        Fibonacci f2 = new Fibonacci(n - 2);
        return f2.compute() + f1.join();
    }
}

class CountTask extends RecursiveTask<Long> {
    private static final int THRESHOLD = 10000;
    private int start;
    private int end;

    public CountTask(int start, int end) {
        this.start = start;
        this.end = end;
    }

    protected Long compute() {
        //System.out.println("Thread ID: " + Thread.currentThread().getId());

        Long sum;

        if ((end - start) <= THRESHOLD) {
            sum = SegmentTest.sum1(start, end);
        } else {
            int middle = (start + end) / 2;
            CountTask leftTask = new CountTask(start, middle);
            CountTask rightTask = new CountTask(middle + 1, end);
            leftTask.fork();
            rightTask.fork();

            Long leftResult = leftTask.join();
            Long rightResult = rightTask.join();

            sum = leftResult + rightResult;
        }

        return sum;
    }

}