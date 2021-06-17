import com.fasterxml.jackson.annotation.JsonProperty;

public class ItemUpdate {

  @JsonProperty("userId")
  public String userId;

  @JsonProperty("itemDescription")
  public String itemDescription;

  @JsonProperty("itemId")
  public String itemId;

  @Override
  public String toString() {
    return "ItemUpdate{" +
        "itemId='" + itemId + '\'' +
        "userId='" + userId + '\'' +
        ", itemDescription='" + itemDescription + '\'' +
        '}';
  }
}