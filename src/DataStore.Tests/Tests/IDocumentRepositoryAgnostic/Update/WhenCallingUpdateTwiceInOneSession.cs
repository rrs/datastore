using System;
using System.Linq;
using DataStore.Models.Messages;
using DataStore.Tests.Models;
using DataStore.Tests.TestHarness;
using Xunit;

namespace DataStore.Tests.Tests.IDocumentRepositoryAgnostic.Update
{
    public class WhenCallingUpdateTwiceInOneSession
    {
        public WhenCallingUpdateTwiceInOneSession()
        {
            // Given
            testHarness = TestHarnessFunctions.GetTestHarness(nameof(WhenCallingUpdateTwiceInOneSession));

            carId = Guid.NewGuid();
            var existingCar = new Car
            {
                id = carId,
                Make = "Volvo"
            };
            testHarness.AddToDatabase(existingCar);

            //When
            testHarness.DataStore.UpdateById<Car>(carId, c => c.Make = "Toyota").Wait();
            testHarness.DataStore.UpdateById<Car>(carId, c => c.Make = "Honda").Wait();
            testHarness.DataStore.CommitChanges().Wait();
        }

        private readonly ITestHarness testHarness;
        private readonly Guid carId;

        [Fact]
        public void ItShouldPersistTheLastChangeToTheDatabase()
        {
            Assert.Equal(2, testHarness.Operations.Count(e => e is UpdateOperation<Car>));
            Assert.Equal(2, testHarness.QueuedWriteOperations.Count(e => e is QueuedUpdateOperation<Car>));
            Assert.Equal("Honda", testHarness.QueryDatabase<Car>(cars => cars.Where(car => car.id == carId)).Single().Make);
            Assert.Equal("Honda", testHarness.DataStore.ReadActiveById<Car>(carId).Result.Make);
        }
    }
}