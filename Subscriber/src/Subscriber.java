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

            IDirectory directoryService = (IDirectory) Naming.lookup("//localhost:1099/DirectoryService");

            List<String> activeBrokers = directoryService.getActiveBrokers("", 0);

            System.out.println("Please enter your name: ");
            subName = scanner.nextLine();

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
                System.out.print("Enter command (subscribe/unsubscribe/exit): ");
                String command = scanner.nextLine();

                if (command.equalsIgnoreCase("exit")) {
                    break;
                } else{
                    System.out.println(command);
                }
                // Handle other commands such as subscribe/unsubscribe
                // Example: broker.subscribeToTopic(subName, topicName);
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