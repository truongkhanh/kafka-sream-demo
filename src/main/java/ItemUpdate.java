import com.fasterxml.jackson.annotation.JsonProperty;

public class ItemUpdate {

  @JsonProperty("userId")
  public String userId;

  @JsonProperty("itemDescription")
  public String itemDescription;

  @Override
  public String toString() {
    return "ItemUpdate{" +
        "userId='" + userId + '\'' +
        ", itemDescription='" + itemDescription + '\'' +
        '}';
  }
}