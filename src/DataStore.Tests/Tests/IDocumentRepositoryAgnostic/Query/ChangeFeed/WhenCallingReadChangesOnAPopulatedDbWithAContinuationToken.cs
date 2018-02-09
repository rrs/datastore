namespace DataStore.Tests.Tests.IDocumentRepositoryAgnostic.Query.ChangeFeed
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using global::DataStore.Models.Messages;
    using global::DataStore.Tests.Models;
    using global::DataStore.Tests.TestHarness;
    using Xunit;

    public class WhenCallingReadChangesOnAPopulatedDbWithAContinuationToken
    {
        private readonly IEnumerable<Car> carsFromSessionAfterToken;

        private readonly ITestHarness testHarness;

        public WhenCallingReadChangesOnAPopulatedDbWithAContinuationToken()
        {
            // Given
            this.testHarness = TestHarnessFunctions.GetTestHarness(nameof(WhenCallingReadChangesOnAPopulatedDbWithAContinuationToken));

            var car = new Car
            {
                id = Guid.NewGuid(),
                Make = "Volvo"
            };
            this.testHarness.AddToDatabase(car);

            var changes = this.testHarness.DataStore.Advanced.ReadChanged<Car>(null).Result;

            var car2 = new Car
            {
                id = Guid.NewGuid(),
                Make = "Toyota"
            };
            this.testHarness.AddToDatabase(car2);

            carsFromSessionAfterToken = this.testHarness.DataStore.Advanced.ReadChanged<Car>(changes.ContinuationToken).Result.Changed;
        }

        [Fact]
        public void ItShouldReturnNothing()
        {
            Assert.Equal(2, this.testHarness.DataStore.ExecutedOperations.Count(e => e is AggregateChangesQueryOperation));
            Assert.Single(carsFromSessionAfterToken);
            Assert.Equal("Toyota", carsFromSessionAfterToken.Single().Make);
        }
    }
}