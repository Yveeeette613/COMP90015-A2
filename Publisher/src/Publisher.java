import Interface.IBroker;
import Interface.IDirectory;
import java.rmi.Naming;
import java.util.List;
import java.util.Scanner;

public class Publisher {
    private static String pubName;
    private static IBroker broker = null;

    public static void main(String[] args) {
        try {
            Scanner scanner = new Scanner(System.in);

            // Connect to DirectoryService
            IDirectory directoryService = (IDirectory) Naming.lookup("//localhost:1099/DirectoryService");

            // Get the list of active brokers
            List<String> activeBrokers = directoryService.getActiveBrokers("", 0);

            System.out.println("Please enter your name: ");
            pubName = scanner.nextLine();

            if (activeBrokers.isEmpty()) {
                System.out.println("No active brokers found.");
                return;
            }

            // Display the list of brokers to the user
            System.out.println("Active brokers:");
//            System.out.println("Please select brokers with less than 5 publishers.");

            for (int i = 0; i < activeBrokers.size(); i++) {
                String brokerAddress = activeBrokers.get(i);
//                IBroker tempBroker = (IBroker) Naming.lookup("//" + brokerAddress + "/Broker");
//                int connectedPublishers = tempBroker.getConnectedPublishers();
//                System.out.println((i + 1) + ". " + brokerAddress + " (Connected Publishers: " + connectedPublishers + ")");
                System.out.println((i + 1) + ". " + brokerAddress);
            }

            // Allow the user to select a broker
            System.out.print("Select a broker to connect to (1-" + activeBrokers.size() + "): ");
            int brokerIndex = Integer.parseInt(scanner.nextLine()) - 1;

            if (brokerIndex < 0 || brokerIndex >= activeBrokers.size()) {
                System.out.println("Invalid selection.");
                return;
            }

            String selectedBroker = activeBrokers.get(brokerIndex);
            String[] brokerParts = selectedBroker.split(":");
            String brokerIP = brokerParts[0];
            int brokerPort = Integer.parseInt(brokerParts[1]);

            // Connect to the selected broker
            broker = (IBroker) Naming.lookup("//" + brokerIP + ":" + brokerPort + "/Broker");

//            // Check if the broker can accept more publishers
//            if (broker.getConnectedPublishers() >= 5) {
//                System.out.println("Broker has reached the maximum number of connected publishers.");
//                return;
//            }

            System.out.println("Connected to broker " + brokerIP + ":" + brokerPort);
            broker.addPublisher(pubName);

            // Add shutdown hook to remove publisher when JVM shuts down
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (broker != null) {
                    try {
                        broker.removePublisher(pubName);
                        System.out.println("Disconnected from broker.");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }));

            // Publish messages
//            while (true) {
////                System.out.print("Enter the message to publish (or 'exit' to quit): ");
////                String message = scanner.nextLine();
////                if (message.equalsIgnoreCase("exit")) {
////                    break;
////                }
////                broker.publishMessage(message);
////                System.out.println("Message published.");
//
//                System.out.print("Enter the topic ID: ");
//                String topicID = scanner.nextLine();
//                System.out.print("Enter the topic Name: ");
//                String topicName = scanner.nextLine();
//                broker.createTopic(pubName, topicName, topicID);
//                System.out.println("Topic created.");
//
//            }

            while (true) {
                System.out.print("Enter command (publish/create/show/delete/exit): ");
                String command = scanner.nextLine();

                if (command.equalsIgnoreCase("publish")){
                    System.out.print("Enter the message to publish (or 'exit' to quit): ");
                    String message = scanner.nextLine();
                    if (message.equalsIgnoreCase("exit")) {
                        break;
                    }
                    broker.publishMessage(message);
                    System.out.println("Message published.");
                } else if (command.equalsIgnoreCase("create")) {
                    System.out.print("Enter the topic ID: ");
                    String topicID = scanner.nextLine();
                    System.out.print("Enter the topic Name: ");
                    String topicName = scanner.nextLine();
                    broker.createTopic(pubName, topicName, topicID);
                    System.out.println("Topic created.");
                } else if (command.equalsIgnoreCase("show")) {
                    System.out.print("Enter the topic ID: ");

                } else if (command.equalsIgnoreCase("current")) {
                    System.out.print("Enter the topic ID: ");
                } else if (command.equalsIgnoreCase("exit")) {
                    break;
                } else{
                    System.out.println(command);
                }
            }


//            // Uncomment the following lines to send a message to the broker
//            System.out.print("Please select command: create, publish, show, delete.");
//            String topic = scanner.nextLine();

            // System.out.print("Enter the message: ");
            // String message = scanner.nextLine();
            //
            // broker.forwardMessage(topic, message);
            // System.out.println("Message sent to broker.");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (broker != null) {
                try {
                    broker.removePublisher(pubName);
                    System.out.println("Disconnected from broker.");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}