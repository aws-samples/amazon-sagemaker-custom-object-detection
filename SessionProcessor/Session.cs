using System;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace SessionProcessor
{
    public class Session
    {
        private readonly string cameraKey;

        public Session(string cameraKey, DateTime started)
        {
            Started = started;
            this.cameraKey = cameraKey;
            Items = new List<Item>();
        }

        [JsonProperty("id")] public string Id => $"{cameraKey}-{Started.Ticks.ToString().Substring(0, 10)}";

        [JsonProperty("started_at")] public DateTime Started { get; set; }

        [JsonProperty("ended_at")] public DateTime Ended { get; set; }

        [JsonProperty("items")] public List<Item> Items { get; set; }

        [JsonProperty("status")]
        public string Status =>
            Started > DateTime.MinValue && Ended == DateTime.MinValue ? "IN_PROGRESS" : "COMPLETED";
    }
}