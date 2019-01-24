using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace SessionProcessor
{
    public class DynamoDBSessionStore : ISessionStore
    {
        public const string TableName = "TrackedSessions";
        private readonly IAmazonDynamoDB dynamoDb = new AmazonDynamoDBClient();

        public async Task PutSession(Session session)
        {
            var item = new Dictionary<string, AttributeValue>
            {
                {"SessionId", new AttributeValue(session.Id)},
                {"Started", new AttributeValue(session.Started.ToString())},
                {"Status", new AttributeValue(session.Status)}
            };

            if (session.Items.Any())
                item.Add("Items", new AttributeValue(session.Items.Select(i => i.Name).ToList()));
            if (session.Ended > DateTime.MinValue)
                item.Add("Ended", new AttributeValue(session.Ended.ToString()));
            
            await dynamoDb.PutItemAsync(
                new PutItemRequest
                {
                    TableName = TableName,
                    Item = item
                });
        }

        public async Task DeleteSession(string sessionId)
        {
            await dynamoDb.DeleteItemAsync(TableName, new Dictionary<string, AttributeValue>
            {
                {"SessionId", new AttributeValue(sessionId)}
            });
        }
    }
}