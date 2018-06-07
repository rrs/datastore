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

    public interface IDataStoreReadTransformOperation<TQuery, TResult> : IDataStoreReadOperation
    {
        Expression<Func<TQuery, TResult>> Select { get; set; }
    }

    public interface IDataStoreReadTransformFromQueryable<TQuery, TResult> : IDataStoreReadFromQueryable<TQuery>, IDataStoreReadTransformOperation<TQuery, TResult>
    {
    }

    public interface IDataStoreReadById : IDataStoreReadOperation
    {
        Guid Id { get; set; }
    }

    public interface IDataStoreReadOperation : IDataStoreOperation, IRequestState
    {
    }


}