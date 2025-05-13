import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ParallelMergeSort {
    private static final int THRESHOLD = 100_000;
    private static final int ARRAY_SIZE = 100_000;
    private final ExecutorService executor;

    public ParallelMergeSort() {
        this.executor = Executors.newFixedThreadPool(4);
    }

    private static int[] generateRandomArray(int size) {
        Random random = new Random();
        int[] array = new int[size];
        for (int i = 0; i < size; i++) {
            array[i] = random.nextInt(1000000);
        }
        return array;
    }

    public void sort(int[] array) throws Exception {
        mergeSort(array, 0, array.length - 1);
        executor.shutdown();
    }

    private void mergeSort(int[] array, int left, int right) throws Exception {
        if (left < right) {
            int mid = left + (right - left) / 2;
            if (right - left + 1 > THRESHOLD) {
                Future<?> leftSort = executor.submit(() -> {
                    try {
                        mergeSort(array, left, mid);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });

                Future<?> rightSort = executor.submit(() -> {
                    try {
                        mergeSort(array, mid + 1, right);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });

                leftSort.get();
                rightSort.get();
            } else {
                sequentialMergeSort(array, left, right);
            }
            merge(array, left, mid, right);
        }
    }

    private void sequentialMergeSort(int[] array, int left, int right) {
        if (left < right) {
            int mid = left + (right - left) / 2;
            sequentialMergeSort(array, left, mid);
            sequentialMergeSort(array, mid + 1, right);
            merge(array, left, mid, right);
        }
    }

    private void merge(int[] array, int left, int mid, int right) {
        int leftSize = mid - left + 1;
        int rightSize = right - mid;
        int[] leftArray = new int[leftSize];
        int[] rightArray = new int[rightSize];

        for (int i = 0; i < leftSize; i++) {
            leftArray[i] = array[left + i];
        }
        for (int j = 0; j < rightSize; j++) {
            rightArray[j] = array[mid + 1 + j];
        }

        int i = 0, j = 0, k = left;
        while (i < leftSize && j < rightSize) {
            if (leftArray[i] <= rightArray[j]) {
                array[k++] = leftArray[i++];
            } else {
                array[k++] = rightArray[j++];
            }
        }

        while (i < leftSize) {
            array[k++] = leftArray[i++];
        }

        while (j < rightSize) {
            array[k++] = rightArray[j++];
        }
    }

    public static void main(String[] args) {
        try {
            int[] array = generateRandomArray(ARRAY_SIZE);
            System.out.println(Arrays.toString(array));
            System.out.println();
            System.out.println();
            System.out.println();
            ParallelMergeSort sorter = new ParallelMergeSort();
            long startTime = System.nanoTime();
            sorter.sort(array);
            long endTime = System.nanoTime();
            double durationMs = (endTime - startTime) / 1_000_000.0;
            
            System.out.println();
            System.out.println();
            System.out.println();
            System.out.println(Arrays.toString(array));
            System.out.println();
            System.out.println();
            System.out.println();
            System.out.printf("Time taken: %.2f ms%n", durationMs);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
