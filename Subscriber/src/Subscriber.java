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

            // Connect to DirectoryService
            IDirectory directoryService = (IDirectory) Naming.lookup("//localhost:1099/DirectoryService");

            // Get the list of active brokers
            List<String> activeBrokers = directoryService.getActiveBrokers("", 0);

            System.out.println("Please enter your name: ");
            subName = scanner.nextLine();

            if (activeBrokers.isEmpty()) {
                System.out.println("No active brokers found.");
                return;
            }

            // Display the list of brokers to the user
            System.out.println("Active brokers:");
            System.out.println("Please select brokers with less than 10 subscribers.");

            for (int i = 0; i < activeBrokers.size(); i++) {
                String brokerAddress = activeBrokers.get(i);
                IBroker tempBroker = (IBroker) Naming.lookup("//" + brokerAddress + "/Broker");
                int connectedSubscribers = tempBroker.getConnectedSubscribers();
                System.out.println((i + 1) + ". " + brokerAddress + " (Connected Subscribers: " + connectedSubscribers + ")");
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

            // Check if the broker can accept more subscribers
            if (broker.getConnectedSubscribers() >= 10) {
                System.out.println("Broker has reached the maximum number of connected subscribers.");
                return;
            }

            System.out.println("Connected to broker " + brokerIP + ":" + brokerPort);
            broker.addSubscriber(subName);

            // Add shutdown hook to remove subscriber when JVM shuts down
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

            // Uncomment the following lines to send a message to the broker
            System.out.print("Please select command: list, sub, current, unsub.");
            String topic = scanner.nextLine();

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
                    broker.removeSubscriber(subName);
                    System.out.println("Disconnected from broker.");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}