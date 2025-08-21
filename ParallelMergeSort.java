import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ParallelMergeSort {
    
    // Threshold to switch to sequential sort (tuned for performance)
    private static final int THRESHOLD = 10_000;
    
    // Size of the array to sort
    private static final int ARRAY_SIZE = 1_000_000;
    
    // ExecutorService for managing thread pool
    private final ExecutorService executor;
    
    public ParallelMergeSort() {
        // Get the number of available processors
        int cores = Runtime.getRuntime().availableProcessors();
        System.out.println("Available cores: " + cores);
        // Create a thread pool with a thread for each core
        this.executor = Executors.newFixedThreadPool(cores);
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
        System.out.println("Starting parallel sort...");
        try {
            parallelMergeSort(array, 0, array.length - 1);
        } finally {
            // Ensure executor is always shut down
            shutdownExecutor();
        }
        System.out.println("Parallel sort completed.");
    }
    
    // Parallel merge sort that limits recursion depth
    private void parallelMergeSort(int[] array, int left, int right) throws Exception {
        if (left < right) {
            int mid = left + (right - left) / 2;
            
            // Only use parallel execution for large enough subarrays
            if (right - left + 1 >= THRESHOLD) {
                // Create tasks for sorting left and right halves
                Future<?> leftSort = executor.submit(() -> {
                    // Use sequential sort for recursive calls to avoid thread explosion
                    sequentialMergeSort(array, left, mid);
                });
                
                Future<?> rightSort = executor.submit(() -> {
                    // Use sequential sort for recursive calls to avoid thread explosion
                    sequentialMergeSort(array, mid + 1, right);
                });
                
                // Wait for both sorting tasks to complete
                leftSort.get();
                rightSort.get();
            } else {
                // For small subarrays, use sequential merge sort
                sequentialMergeSort(array, left, right);
                return; // No need to merge again
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
        System.arraycopy(array, left, leftArray, 0, leftSize);
        System.arraycopy(array, mid + 1, rightArray, 0, rightSize);
        
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
    
    // Properly shutdown the executor
    private void shutdownExecutor() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    System.err.println("Pool did not terminate");
                }
            }
        } catch (InterruptedException ie) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
    
    // Verify array is sorted
    private static boolean isSorted(int[] array) {
        for (int i = 1; i < array.length; i++) {
            if (array[i] < array[i - 1]) {
                return false;
            }
        }
        return true;
    }
    
    public static void main(String[] args) {
        try {
            // Read boolean input for demonstration mode
            System.out.print("Run demonstration (true/false)? ");
            Scanner scanner = new Scanner(System.in);
            boolean demonstrate = scanner.nextBoolean();
            scanner.close();
            
            // Generate a large random array
            System.out.println("Generating random array of size " + ARRAY_SIZE + "...");
            int[] originalArray = generateRandomArray(ARRAY_SIZE);
            System.out.println("Array generated.");
            
            // Run parallel merge sort
            ParallelMergeSort parallelSorter = new ParallelMergeSort();
            int[] parallelArray = copyArray(originalArray);
            long startTime = System.nanoTime();
            parallelSorter.parallelSort(parallelArray);
            long endTime = System.nanoTime();
            double parallelDurationMs = (endTime - startTime) / 1_000_000.0;
            System.out.printf("Parallel Merge Sort time: %.2f ms%n", parallelDurationMs);
            System.out.println("Parallel sort result is sorted: " + isSorted(parallelArray));
            
            // If demonstration is enabled, run sequential merge sort
            if (demonstrate) {
                System.out.println("Starting sequential sort...");
                ParallelMergeSort sequentialSorter = new ParallelMergeSort();
                int[] sequentialArray = copyArray(originalArray);
                startTime = System.nanoTime();
                sequentialSorter.sequentialMergeSort(sequentialArray, 0, ARRAY_SIZE - 1);
                sequentialSorter.shutdownExecutor(); // Clean shutdown
                endTime = System.nanoTime();
                double sequentialDurationMs = (endTime - startTime) / 1_000_000.0;
                System.out.printf("Sequential Merge Sort time: %.2f ms%n", sequentialDurationMs);
                System.out.println("Sequential sort result is sorted: " + isSorted(sequentialArray));
                
                // Calculate speedup
                double speedup = sequentialDurationMs / parallelDurationMs;
                System.out.printf("Speedup: %.2fx%n", speedup);
            }
            
            System.out.println("Program completed successfully.");
            
        } catch (Exception e) {
            System.err.println("An error occurred:");
            e.printStackTrace();
        }
    }
}