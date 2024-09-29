// Subscriber/src/MessageReceiver.java
import Interface.IBroker;

public class MessageReceiver implements Runnable {
    private IBroker broker;

    public MessageReceiver(IBroker broker) {
        this.broker = broker;
    }

    @Override
    public void run() {
        try {
            while (true) {
                String message = broker.receiveMessage();
                if (message != null) {
                    System.out.println("Received message: " + message);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}