import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Topic {
    private String pubName;
    private String topicName;
    private String topicId;
    private List<String> subscribersList;

    public Topic(String pubName, String topicName, String topicId) {
        this.pubName = pubName;
        this.topicName = topicName;
        this.topicId = topicId;
        this.subscribersList = new ArrayList<String>();
    }

    public String getTopicId() {
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


    public String findTopicName(String topicId) {
        return topicName;
    }

}