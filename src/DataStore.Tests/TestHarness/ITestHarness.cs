namespace DataStore.Tests.TestHarness
{
    using CircuitBoard.Messages;
    using global::DataStore.Interfaces.LowLevel;
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;

    public interface ITestHarness
    {
        List<IMessage> AllMessages { get; }

        DataStore DataStore { get; }

        //add to the underlying db directly (i.e. not via datastore)
        void AddToDatabase<T>(T aggregate) where T : class, IAggregate, new();

        //query the underlying db directly (i.e. not via datastore)
        IEnumerable<T> QueryDatabase<T>(Expression<Func<T, bool>> query = null) where T : class, IAggregate, new();
    }
}