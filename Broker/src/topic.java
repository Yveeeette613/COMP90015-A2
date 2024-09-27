import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class topic {
    private String pubName;
    private String topicName;
    private UUID topicId;
    private List<String> subscribersList;

    public topic(String pubName, String topicName) {
        this.pubName = pubName;
        this.topicName = topicName;
        this.topicId = UUID.randomUUID(); // Generate a UUID
        this.subscribersList = new ArrayList<String>();
    }

    public UUID getTopicId() {
        return topicId;
    }

    // Getters and setters for pubName and topicName
    public String getPubName() {
        return pubName;
    }

    public void setPubName(String pubName) {
        this.pubName = pubName;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public List<String> getSubscribersList() {
        return subscribersList;
    }

}