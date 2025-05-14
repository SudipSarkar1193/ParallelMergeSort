import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ParallelMergeSort {
    // Threshold to switch to sequential sort (tuned for performance)
    private static final int THRESHOLD = 100_000;
    // Size of the array to sort
    private static final int ARRAY_SIZE = 100_000;
    // ExecutorService for managing thread pool
    private final ExecutorService executor;

    public ParallelMergeSort() {
        // Create a fixed thread pool (4 threads, typical for quad-core CPUs)
        this.executor = Executors.newFixedThreadPool(4);
    }

    // Generates a random array of specified size
    private static int[] generateRandomArray(int size) {
        Random random = new Random();
        int[] array = new int[size];
        for (int i = 0; i < size; i++) {
            array[i] = random.nextInt(1000000); // Random integers up to 1 million
        }
        return array;
    }

    // Copies an array to create an identical instance
    private static int[] copyArray(int[] source) {
        int[] copy = new int[source.length];
        System.arraycopy(source, 0, copy, 0, source.length);
        return copy;
    }

    // Public method for parallel merge sort
    public void parallelSort(int[] array) throws Exception {
        mergeSort(array, 0, array.length - 1);
        // Shutdown executor after sorting is complete
        executor.shutdown();
    }

    // Recursive mergeSort that decides between parallel and sequential
    private void mergeSort(int[] array, int left, int right) throws Exception {
        if (left < right) {
            int mid = left + (right - left) / 2;

            // If subarray size is at least the threshold, use parallel sorting
            if (right - left + 1 >= THRESHOLD) {
                // Create tasks for sorting left and right halves
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

                // Wait for both sorting tasks to complete
                leftSort.get();
                rightSort.get();
            } else {
                // For small subarrays, use sequential merge sort
                sequentialMergeSort(array, left, right);
            }

            // Merge the sorted halves
            merge(array, left, mid, right);
        }
    }

    // Sequential merge sort for small subarrays or comparison
    public void sequentialMergeSort(int[] array, int left, int right) {
        // Base case: if one or zero elements, already sorted
        if (left < right) {
            // Find the middle point
            int mid = left + (right - left) / 2;
            // Recursively sort left half
            sequentialMergeSort(array, left, mid);
            // Recursively sort right half
            sequentialMergeSort(array, mid + 1, right);
            // Merge the sorted halves
            merge(array, left, mid, right);
        }
    }

    // Merges two sorted subarrays: [left, mid] and [mid+1, right]
    private void merge(int[] array, int left, int mid, int right) {
        // Calculate sizes of two subarrays
        int leftSize = mid - left + 1;
        int rightSize = right - mid;

        // Create temporary arrays for left and right subarrays
        int[] leftArray = new int[leftSize];
        int[] rightArray = new int[rightSize];

        // Copy data to temporary arrays
        for (int i = 0; i < leftSize; i++) {
            leftArray[i] = array[left + i];
        }
        for (int j = 0; j < rightSize; j++) {
            rightArray[j] = array[mid + 1 + j];
        }

        // Merge the temporary arrays back into array[left..right]
        int i = 0; // Index for left subarray
        int j = 0; // Index for right subarray
        int k = left; // Index for merged array

        while (i < leftSize && j < rightSize) {
            if (leftArray[i] <= rightArray[j]) {
                array[k] = leftArray[i];
                i++;
            } else {
                array[k] = rightArray[j];
                j++;
            }
            k++;
        }

        // Copy remaining elements of leftArray, if any
        while (i < leftSize) {
            array[k] = leftArray[i];
            i++;
            k++;
        }

        // Copy remaining elements of rightArray, if any
        while (j < rightSize) {
            array[k] = rightArray[j];
            j++;
            k++;
        }
    }

    public static void main(String[] args) {
        try {
            // Read boolean input for demonstration mode
            System.out.print("Run demonstration (true/false)? ");
            Scanner scanner = new Scanner(System.in);
            boolean demonstrate = scanner.nextBoolean();
            scanner.close();

            // Generate a large random array
            int[] originalArray = generateRandomArray(ARRAY_SIZE);

            // Run parallel merge sort
            ParallelMergeSort parallelSorter = new ParallelMergeSort();
            long startTime = System.nanoTime();
            parallelSorter.parallelSort(copyArray(originalArray));
            long endTime = System.nanoTime();
            double parallelDurationMs = (endTime - startTime) / 1_000_000.0;
            System.out.printf("Parallel Merge Sort time: %.2f ms%n", parallelDurationMs);

            // If demonstration is enabled, run sequential merge sort
            if (demonstrate) {
                ParallelMergeSort sequentialSorter = new ParallelMergeSort();
                startTime = System.nanoTime();
                sequentialSorter.sequentialMergeSort(copyArray(originalArray), 0, ARRAY_SIZE - 1);
                endTime = System.nanoTime();
                double sequentialDurationMs = (endTime - startTime) / 1_000_000.0;
                System.out.printf("Sequential Merge Sort time: %.2f ms%n", sequentialDurationMs);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}