using CircuitBoard.Messages;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DataStore
{
    static class QueuedStateChangeHelper
    {
        public static Task Iterate(IEnumerable<IQueuedStateChange> changes)
        {
            var task = TaskShim.CompletedTask;
            if (changes.Any())
            {
                var query = from action in changes
                            select (task = task.ContinueWith(t => action.CommitClosure()));
                task = query.Last();
            }

            return task;
        }
    }
}
