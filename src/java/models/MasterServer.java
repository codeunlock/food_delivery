import java.io.*;
import java.net.*;
import java.util.*;

public class MasterServer {
    private static final int PORT = 54440;
    private static List<WorkerHandler> workers = new ArrayList<>();
    private static Queue<String> orderQueue = new LinkedList<>();
    
    // Data structures for food shops and products.
    // Using a Map keyed by an integer id.
    private static Map<Integer, FoodShop> foodShops = new HashMap<>();
    private static int foodShopCounter = 1;
    
    // Class to represent a food shop.
    static class FoodShop {
        int id;
        String name;
        List<Product> products = new ArrayList<>();
        
        FoodShop(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }
    
    // Class to represent a product.
    static class Product {
        int id;
        String name;
        double price;
        
        Product(int id, String name, double price) {
            this.id = id;
            this.name = name;
            this.price = price;
        }
    }
    
    public static void main(String[] args) {
        // Start the admin console thread.
        new Thread(() -> adminConsole()).start();
        
        // Start the server socket to accept worker and client connections.
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
    
    // The admin console allows you to add/remove food shops and products.
    private static void adminConsole() {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println("\n----- Admin Menu -----");
            System.out.println("1. Add Food Shop");
            System.out.println("2. Remove Food Shop");
            System.out.println("3. List Food Shops");
            System.out.println("4. Add Product to Food Shop");
            System.out.println("5. Remove Product from Food Shop");
            System.out.println("6. List Products for Food Shop");
            System.out.println("0. Exit Admin Console");
            System.out.print("Choose an option: ");
            int option = scanner.nextInt();
            scanner.nextLine(); // Consume newline
            
            switch (option) {
                case 1:
                    System.out.print("Enter food shop name: ");
                    String shopName = scanner.nextLine();
                    int shopId = foodShopCounter++;
                    FoodShop newShop = new FoodShop(shopId, shopName);
                    synchronized (foodShops) {
                        foodShops.put(shopId, newShop);
                    }
                    System.out.println("Food shop added with id: " + shopId);
                    break;
                case 2:
                    System.out.print("Enter food shop id to remove: ");
                    int removeId = scanner.nextInt();
                    scanner.nextLine();
                    synchronized (foodShops) {
                        if (foodShops.containsKey(removeId)) {
                            foodShops.remove(removeId);
                            System.out.println("Food shop removed.");
                        } else {
                            System.out.println("Food shop not found.");
                        }
                    }
                    break;
                case 3:
                    System.out.println("Listing Food Shops:");
                    synchronized (foodShops) {
                        if (foodShops.isEmpty()) {
                            System.out.println("No food shops available.");
                        } else {
                            for (FoodShop shop : foodShops.values()) {
                                System.out.println("ID: " + shop.id + ", Name: " + shop.name);
                            }
                        }
                    }
                    break;
                case 4:
                    System.out.print("Enter food shop id: ");
                    int shopIdForProduct = scanner.nextInt();
                    scanner.nextLine();
                    synchronized (foodShops) {
                        if (foodShops.containsKey(shopIdForProduct)) {
                            FoodShop shop = foodShops.get(shopIdForProduct);
                            System.out.print("Enter product name: ");
                            String productName = scanner.nextLine();
                            System.out.print("Enter product price: ");
                            double price = scanner.nextDouble();
                            scanner.nextLine();
                            int productId = shop.products.size() + 1; // Simple id generation
                            Product newProduct = new Product(productId, productName, price);
                            shop.products.add(newProduct);
                            System.out.println("Product added to shop " + shop.name);
                        } else {
                            System.out.println("Food shop not found.");
                        }
                    }
                    break;
                case 5:
                    System.out.print("Enter food shop id: ");
                    int shopIdForRemove = scanner.nextInt();
                    scanner.nextLine();
                    synchronized (foodShops) {
                        if (foodShops.containsKey(shopIdForRemove)) {
                            FoodShop shop = foodShops.get(shopIdForRemove);
                            System.out.print("Enter product id to remove: ");
                            int productIdToRemove = scanner.nextInt();
                            scanner.nextLine();
                            boolean removed = shop.products.removeIf(product -> product.id == productIdToRemove);
                            if (removed)
                                System.out.println("Product removed from shop " + shop.name);
                            else
                                System.out.println("Product not found in shop " + shop.name);
                        } else {
                            System.out.println("Food shop not found.");
                        }
                    }
                    break;
                case 6:
                    System.out.print("Enter food shop id: ");
                    int shopIdForList = scanner.nextInt();
                    scanner.nextLine();
                    synchronized (foodShops) {
                        if (foodShops.containsKey(shopIdForList)) {
                            FoodShop shop = foodShops.get(shopIdForList);
                            System.out.println("Products for shop " + shop.name + ":");
                            if (shop.products.isEmpty()) {
                                System.out.println("No products available.");
                            } else {
                                for (Product product : shop.products) {
                                    System.out.println("ID: " + product.id + ", Name: " + product.name + ", Price: " + product.price);
                                }
                            }
                        } else {
                            System.out.println("Food shop not found.");
                        }
                    }
                    break;
                case 0:
                    System.out.println("Exiting Admin Console...");
                    return;
                default:
                    System.out.println("Invalid option. Try again.");
                    break;
            }
        }
    }
    
    // Handles client orders.
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
    
    // Handles communication with worker nodes.
    public static class WorkerHandler implements Runnable {
        private Socket socket;
        
        public WorkerHandler(Socket socket) {
            this.socket = socket;
            try {
                socket.setSoTimeout(1000); // Set a timeout to help detect disconnects.
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
                    try {
                        String message = in.readLine();
                        if (message == null) {
                            throw new IOException("Worker disconnected (null read)");
                        }
                    } catch (SocketTimeoutException ste) {
                        // Expected if no data is sent; continue to check orders.
                    }
                    
                    String order = null;
                    synchronized (orderQueue) {
                        if (orderQueue.isEmpty()) {
                            orderQueue.wait(1000);
                        }
                        if (!orderQueue.isEmpty()) {
                            order = orderQueue.poll();
                        }
                    }
                    
                    if (order != null) {
                        out.println(order);
                        if (out.checkError()) {
                            throw new IOException("Error in output stream");
                        }
                        System.out.println("Assigned order to worker: " + order);
                    }
                }
            } catch (IOException | InterruptedException e) {
                // Worker disconnected or encountered an error.
            } finally {
                synchronized (workers) {
                    workers.remove(this);
                    System.out.println("Worker disconnected. Total workers: " + workers.size());
                }
                try {
                    socket.close();
                } catch (IOException e) {
                    // Ignore closing errors.
                }
            }
        }
    }
}
