using Newtonsoft.Json;

namespace SessionProcessor
{
    public class Item
    {
        [JsonProperty("name")] public string Name { get; set; }

        [JsonProperty("price")] public decimal Price { get; set; }
    }
}