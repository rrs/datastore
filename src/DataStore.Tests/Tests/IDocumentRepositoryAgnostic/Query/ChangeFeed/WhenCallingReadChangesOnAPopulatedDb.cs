namespace DataStore.Tests.Tests.IDocumentRepositoryAgnostic.Query.ChangeFeed
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using global::DataStore.Models.Messages;
    using global::DataStore.Tests.Models;
    using global::DataStore.Tests.TestHarness;
    using Xunit;

    public class WhenCallingReadChangesOnAPopulatedDb
    {
        private readonly IEnumerable<Car> carsFromSession;

        private readonly ITestHarness testHarness;

        public WhenCallingReadChangesOnAPopulatedDb()
        {
            // Given
            this.testHarness = TestHarnessFunctions.GetTestHarness(nameof(WhenCallingReadChangesOnAPopulatedDb));

            var car = new Car
            {
                id = Guid.NewGuid(),
                Active = false,
                Make = "Volvo"
            };
            this.testHarness.AddToDatabase(car);

            this.carsFromSession = this.testHarness.DataStore.Advanced.ReadChanged<Car>(null).Result.Changed;
        }

        [Fact]
        public void ItShouldReturnNothing()
        {
            Assert.NotNull(this.testHarness.DataStore.ExecutedOperations.SingleOrDefault(e => e is AggregateChangesQueryOperation));
            Assert.Single(carsFromSession);
        }
    }
}