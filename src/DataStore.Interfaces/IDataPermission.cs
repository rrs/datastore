﻿namespace DataStore.Interfaces
{
    using CircuitBoard;
    using CircuitBoard.Permissions;
    using DataStore.Interfaces.LowLevel;

    public interface IDataPermission : IApplicationPermission
    {
        IReadOnlyList<IScopeReference> PermissionScope { get; set; }
    }
}