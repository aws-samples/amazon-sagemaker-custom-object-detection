using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace SessionProcessor
{
    public class WebServiceSessionStore : ISessionStore
    {
        public WebServiceSessionStore()
        {
            
        }


        public WebServiceSessionStore(
            string endpoint = "https://iv91699rh8.execute-api.us-west-2.amazonaws.com/Prod/sessions")
        {
            Endpoint = endpoint;
        }

        public string Endpoint { get; set; }

        public async Task PutSession(Session session)
        {
            var json = JsonConvert.SerializeObject(session);
            using (var httpClient = new HttpClient())
            {
                await httpClient.PostAsync(
                    Endpoint,
                    new StringContent(json)
                        {Headers = {ContentType = new MediaTypeHeaderValue("application/json")}});
            }
        }

        public async Task DeleteSession(string sessionId)
        {
            var requestUrl = $"{Endpoint}/object/{sessionId}";
            using (var httpClient = new HttpClient())
            {
                await httpClient.DeleteAsync(requestUrl);
            }
        }
    }
}