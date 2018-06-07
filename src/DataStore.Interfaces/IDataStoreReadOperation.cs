namespace DataStore.Interfaces
{
    using System;
    using System.Linq;
    using System.Linq.Expressions;
    using CircuitBoard.Messages;

    public interface IDataStoreReadFromQueryable<T> : IDataStoreReadOperation
    {
        Expression<Func<T, bool>> Query { get; set; }
    }

    public interface IDataStoreReadById : IDataStoreReadOperation
    {
        Guid Id { get; set; }
    }

    public interface IDataStoreReadOperation : IDataStoreOperation, IRequestState
    {
    }

    public interface IDataStoreReadTransformOperation<TQuery, TResult> : IDataStoreReadFromQueryable<TQuery>
    {
        Expression<Func<TQuery, TResult>> Select { get; set; }
    }
}