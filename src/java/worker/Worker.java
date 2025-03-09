import java.io.*;
import java.net.*;
import java.util.Scanner;

public class Worker {
    private static final String MASTER_HOST = "localhost";
    private static final int MASTER_PORT = 54440;

    public static void main(String[] args) {
        try (Socket socket = new Socket(MASTER_HOST, MASTER_PORT);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             Scanner scanner = new Scanner(System.in)) {
            
            System.out.println("Worker connected to Master on port " + MASTER_PORT);
            out.println("READY"); // Ενημερώνει τον Master ότι είναι διαθέσιμος

            while (true) {
                System.out.println("\n--- Worker Menu ---");
                System.out.println("1. Request New Order");
                System.out.println("2. Exit");
                System.out.print("Choose an option: ");
                
                int choice = scanner.nextInt();
                scanner.nextLine(); // Consume newline
                
                if (choice == 1) {
                    System.out.println("Waiting for an order...");
                    String order = in.readLine();
                    if (order == null) {
                        System.out.println("No orders available. Try again later.");
                    } else {
                        System.out.println("Received: " + order);
                        processOrder(order);
                        out.println("COMPLETED: " + order);
                    }
                } else if (choice == 2) {
                    System.out.println("Exiting...");
                    break;
                } else {
                    System.out.println("Invalid choice. Please try again.");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void processOrder(String order) {
        try {
            System.out.println("Processing: " + order);
            Thread.sleep(2000); // Simulating order processing time
            System.out.println("Completed: " + order);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}