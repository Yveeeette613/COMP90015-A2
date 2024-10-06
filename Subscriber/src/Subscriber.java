// Subscriber/src/Subscriber.java
import Interface.IBroker;
import Interface.IDirectory;
import java.rmi.Naming;
import java.util.List;
import java.util.Scanner;

public class Subscriber {
    private static String subName;
    private static IBroker broker = null;

    public static void main(String[] args) {
        try {
            Scanner scanner = new Scanner(System.in);
            String directoryIP = "";
            int directoryPort = 0;
            boolean validInput = false;

            while (!validInput) {
                System.out.println("Please enter username directory_ip directory_port: ");
                String input = scanner.nextLine();
                String[] parts = input.split(" ");

                if (parts.length == 3) {
                    try {
                        subName = parts[0];
                        directoryIP = parts[1];
                        directoryPort = Integer.parseInt(parts[2]);
                        validInput = true;
                    } catch (NumberFormatException e) {
                        System.out.println("Invalid port number. Please enter a valid username, IP address, and port number.");
                    }
                } else {
                    System.out.println("Invalid format. Please enter the username, IP address, and port number in the format: username directory_ip directory_port");
                }
            }

            IDirectory directoryService = (IDirectory) Naming.lookup("//" + directoryIP + ":" + directoryPort + "/DirectoryService");
            List<String> activeBrokers = directoryService.getActiveBrokers("", 0);

            if (activeBrokers.isEmpty()) {
                System.out.println("No active brokers found.");
                return;
            }

            System.out.println("Active brokers:");
            for (int i = 0; i < activeBrokers.size(); i++) {
                String brokerAddress = activeBrokers.get(i);
                System.out.println((i + 1) + ". " + brokerAddress);
            }

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

            broker = (IBroker) Naming.lookup("//" + brokerIP + ":" + brokerPort + "/Broker");

            System.out.println("Connected to broker " + brokerIP + ":" + brokerPort);
            broker.addSubscriber(subName);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (broker != null) {
                    try {
                        broker.removeSubscriber(subName);
                        System.out.println("Disconnected from broker.");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }));

            // Start the message receiver thread
            Thread messageReceiverThread = new Thread(new MessageReceiver(broker));
            messageReceiverThread.start();

            // Main thread for other functions
            while (true) {
                System.out.print("Enter command (topic/subscribe/unsubscribe/current/exit): ");
                String command = scanner.nextLine();

                if (command.equalsIgnoreCase("topic")){
                    List<String> topicList = broker.getTopicList(null);
                    for (String topic: topicList){
                        System.out.println(topic);
                    }
                } else if (command.equalsIgnoreCase("subscribe")) {
                    System.out.print("Topic ID: ");
                    String topicID = scanner.nextLine();
                    broker.subscribeTopic(topicID, subName,null);
                } else if (command.equalsIgnoreCase("unsubscribe")) {
                    System.out.print("Topic ID: ");
                    String topicID = scanner.nextLine();
                    broker.unsubscribeTopic(topicID, subName,null);
                } else if (command.equalsIgnoreCase("current")) {
                    List<String> subscribedTopicList = broker.getSubscribedTopicList(subName, null);
                    for (String topic: subscribedTopicList){
                        System.out.println(topic);
                    }
                } else if (command.equalsIgnoreCase("exit")) {
                    break;
                } else{
                    System.out.println(command);
                }
            }



        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (broker != null) {
                try {
                    broker.removeSubscriber(subName);
                    System.out.println("Disconnected from broker.");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}