import java.io.*;
import java.net.*;
import java.util.*;

public class MasterServer {
    private static final int PORT = 54440;
    private static List<WorkerHandler> workers = new ArrayList<>();
    private static Queue<String> orderQueue = new LinkedList<>();

    public static void main(String[] args) {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("Master Server started on port " + PORT);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                // Read the first message to decide if it's a worker or a client
                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                String firstMessage = in.readLine();

                if ("READY".equals(firstMessage)) {
                    WorkerHandler worker = new WorkerHandler(clientSocket);
                    new Thread(worker).start();
                } else {
                    new Thread(new ClientHandler(clientSocket, firstMessage)).start();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static class ClientHandler implements Runnable {
        private Socket socket;
        private String initialMessage;

        public ClientHandler(Socket socket, String initialMessage) {
            this.socket = socket;
            this.initialMessage = initialMessage;
        }

        @Override
        public void run() {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
                
                if (initialMessage != null) {
                    synchronized (orderQueue) {
                        orderQueue.add(initialMessage);
                        orderQueue.notify();
                    }
                    out.println("Order received: " + initialMessage);
                }

                String input;
                while ((input = in.readLine()) != null) {
                    synchronized (orderQueue) {
                        orderQueue.add(input);
                        orderQueue.notify();
                    }
                    out.println("Order received: " + input);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class WorkerHandler implements Runnable {
        private Socket socket;

        public WorkerHandler(Socket socket) {
            this.socket = socket;
            try {
                // Set a timeout so that readLine() won’t block forever.
                socket.setSoTimeout(1000);
            } catch (SocketException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            PrintWriter out = null;
            try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                 PrintWriter localOut = new PrintWriter(socket.getOutputStream(), true)) {
                out = localOut;
                synchronized (workers) {
                    workers.add(this);
                    System.out.println("Worker connected. Total workers: " + workers.size());
                }
                
                while (true) {
                    // First, try reading from the worker to detect disconnection.
                    try {
                        String message = in.readLine();
                        if (message == null) {
                            // If null is returned, the worker has disconnected.
                            throw new IOException("Worker disconnected (null read)");
                        }
                        // Optionally, handle any messages from the worker (e.g., COMPLETED messages).
                        // System.out.println("Received from worker: " + message);
                    } catch (SocketTimeoutException ste) {
                        // Expected if no data is sent; this is our opportunity to check orders.
                    }
                    
                    // Now, check if there's an order waiting.
                    String order = null;
                    synchronized (orderQueue) {
                        if (orderQueue.isEmpty()) {
                            // Wait briefly (with timeout) so we can loop back to check connection.
                            orderQueue.wait(1000);
                        }
                        if (!orderQueue.isEmpty()) {
                            order = orderQueue.poll();
                        }
                    }
                    
                    if (order != null) {
                        out.println(order);
                        // Immediately check if writing caused an error.
                        if (out.checkError()) {
                            throw new IOException("Error detected in output stream");
                        }
                        System.out.println("Assigned order to worker: " + order);
                    }
                }
            } catch (IOException | InterruptedException e) {
                // When an exception occurs, we assume the worker has disconnected.
            }
            } finally {
                synchronized (workers) {
                    workers.remove(this);
                    System.out.println("Worker disconnected. Total workers: " + workers.size());
                }
                try {
                    socket.close();
                } catch (IOException e) {
                    // Ignore errors on close.
                }
            }
        }
    }
}
