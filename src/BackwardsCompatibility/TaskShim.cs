using System.Threading.Tasks;

namespace DataStore
{
    public static class TaskShim
    {
        public static Task<T> FromResult<T>(T value)
        {
            var tcs = new TaskCompletionSource<T>();
            tcs.SetResult(value);
            return tcs.Task;
        }

        public static Task CompletedTask => CompletedTaskImpl();

        private static Task CompletedTaskImpl()
        {
            var tcs = new TaskCompletionSource<object>();
            tcs.SetResult(null);
            return tcs.Task;
        }
    }
}
