package edu.coursera.parallel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;

/**
 * Class wrapping methods for implementing reciprocal array sum in parallel.
 */
public final class ReciprocalArraySum {

    /**
     * Default constructor.
     */
    private ReciprocalArraySum() {
    }

    /**
     * Sequentially compute the sum of the reciprocal values for a given array.
     *
     * @param input Input array
     * @return The sum of the reciprocals of the array input
     */
    protected static double seqArraySum(final double[] input) {
        double sum = 0;

        // Compute sum of reciprocals of array elements
        for (double v : input) {
            sum += 1 / v;
        }

        return sum;
    }

    /**
     * Computes the size of each chunk, given the number of chunks to create
     * across a given number of elements.
     *
     * @param nChunks   The number of chunks to create
     * @param nElements The number of elements to chunk across
     * @return The default chunk size
     */
    private static int getChunkSize(final int nChunks, final int nElements) {
        // Integer ceil
        return (nElements + nChunks - 1) / nChunks;
    }

    /**
     * Computes the inclusive element index that the provided chunk starts at,
     * given there are a certain number of chunks.
     *
     * @param chunk     The chunk to compute the start of
     * @param nChunks   The number of chunks created
     * @param nElements The number of elements to chunk across
     * @return The inclusive index that this chunk starts at in the set of
     * nElements
     */
    private static int getChunkStartInclusive(final int chunk,
                                              final int nChunks, final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        return chunk * chunkSize;
    }

    /**
     * Computes the exclusive element index that the provided chunk ends at,
     * given there are a certain number of chunks.
     *
     * @param chunk     The chunk to compute the end of
     * @param nChunks   The number of chunks created
     * @param nElements The number of elements to chunk across
     * @return The exclusive end index for this chunk
     */
    private static int getChunkEndExclusive(final int chunk, final int nChunks,
                                            final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        final int end = (chunk + 1) * chunkSize;
        return Math.min(end, nElements);
    }

    /**
     * This class stub can be filled in to implement the body of each task
     * created to perform reciprocal array sum in parallel.
     */
    private static class ReciprocalArraySumTask extends RecursiveAction {
        private static final int SEQUENTIAL_THRESHOLD = 20000;
        /**
         * Starting index for traversal done by this task.
         */
        private final int lo;
        /**
         * Ending index for traversal done by this task.
         */
        private final int hi;
        /**
         * Input array to reciprocal sum.
         */
        private final double[] input;
        /**
         * Intermediate value produced by this task.
         */
        private double value;

        /**
         * Constructor.
         *
         * @param lo       Set the starting index to begin
         *                 parallel traversal at.
         * @param hi       Set ending index for parallel traversal.
         * @param setInput Input values
         */
        ReciprocalArraySumTask(final int lo,
                               final int hi, final double[] setInput) {
            this.lo = lo;
            this.hi = hi;
            this.input = setInput;
        }

        /**
         * Getter for the value produced by this task.
         *
         * @return Value produced by this task
         */
        public double getValue() {
            return value;
        }

        @Override
        protected void compute() {
            if (hi - lo <= SEQUENTIAL_THRESHOLD) {
                for (int i = lo; i < hi; i++) {
                    value += 1 / input[i];
                }
            } else {
                int mid = lo + (hi - lo) / 2;
                ReciprocalArraySumTask left = new ReciprocalArraySumTask(lo, mid, input);
                ReciprocalArraySumTask right = new ReciprocalArraySumTask(mid, hi, input);
                left.fork();
                right.compute();
                left.join();
                value = left.getValue() + right.getValue();
            }
        }
    }

    /**
     * TODO: Modify this method to compute the same reciprocal sum as
     * seqArraySum, but use two tasks running in parallel under the Java Fork
     * Join framework. You may assume that the length of the input array is
     * evenly divisible by 2.
     *
     * @param input Input array
     * @return The sum of the reciprocals of the array input
     */
    protected static double parArraySum(final double[] input) {
        int len = input.length;
        assert len % 2 == 0;

        ReciprocalArraySumTask task = new ReciprocalArraySumTask(0, len, input);
        ForkJoinPool.commonPool().invoke(task);
        return task.getValue();
    }

    /**
     * TODO: Extend the work you did to implement parArraySum to use a set
     * number of tasks to compute the reciprocal array sum. You may find the
     * above utilities getChunkStartInclusive and getChunkEndExclusive helpful
     * in computing the range of element indices that belong to each chunk.
     *
     * @param input    Input array
     * @param numTasks The number of tasks to create
     * @return The sum of the reciprocals of the array input
     */
    protected static double parManyTaskArraySum(final double[] input,
                                                final int numTasks) {
        double sum = 0;
        int len = input.length;
        ReciprocalArraySumTask[] tasks = new ReciprocalArraySumTask[numTasks];
        for (int i = 0; i < numTasks; i++) {
            int start = getChunkStartInclusive(i, numTasks, len);
            int end = getChunkEndExclusive(i, numTasks, len);
            ReciprocalArraySumTask task = new ReciprocalArraySumTask(start, end, input);
            tasks[i] = task;
        }
        ForkJoinTask.invokeAll(tasks);

        for (ReciprocalArraySumTask task :
                tasks) {
            sum += task.getValue();
        }
        return sum;
    }
}
