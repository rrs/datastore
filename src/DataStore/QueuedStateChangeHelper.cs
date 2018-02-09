namespace DataStore
{
    using CircuitBoard.Messages;
    using Rrs.TaskShim;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    static class QueuedStateChangeHelper
    {
        public static Task Iterate(IEnumerable<IQueuedStateChange> changes)
        {
            var task = Tap.CompletedTask;
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
