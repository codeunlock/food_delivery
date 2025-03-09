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
        }

        @Override
        public void run() {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

                synchronized (workers) {
                    workers.add(this);
                    System.out.println("Worker connected. Total workers: " + workers.size());
                }

                while (true) {
                    String order;
                    synchronized (orderQueue) {
                        while (orderQueue.isEmpty()) {
                            try {
                                orderQueue.wait();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        order = orderQueue.poll();
                    }
                    out.println(order);
                    System.out.println("Assigned order to worker: " + order);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
