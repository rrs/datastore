namespace DataStore.Tests.Tests.IDocumentRepositoryAgnostic.Query.ChangeFeed
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using global::DataStore.Models.Messages;
    using global::DataStore.Tests.Models;
    using global::DataStore.Tests.TestHarness;
    using Xunit;

    public class WhenCallingReadChangesOnEmptyDb
    {
        private readonly IEnumerable<Car> carsFromSession;

        private readonly ITestHarness testHarness;

        public WhenCallingReadChangesOnEmptyDb()
        {
            // Given
            this.testHarness = TestHarnessFunctions.GetTestHarness(nameof(WhenCallingReadChangesOnEmptyDb));

            this.carsFromSession = this.testHarness.DataStore.Advanced.ReadChanged<Car>(null).Result.Changed;
        }

        [Fact]
        public void ItShouldReturnNothing()
        {
            Assert.NotNull(this.testHarness.DataStore.ExecutedOperations.SingleOrDefault(e => e is AggregateChangesQueryOperation));
            Assert.Empty(carsFromSession);
        }
    }
}