using System.Threading.Tasks;

namespace SessionProcessor
{
    public interface ISessionStore
    {


        Task PutSession(Session session);
        Task DeleteSession(string sessionId);
    }
}