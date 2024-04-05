using Azure.Core;
using Azure.Identity;
using Databricks.Sql.Net.Authentication;
using Databricks.Sql.Net.Options;
using Moq;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Databricks.Sql.Net.Tests
{
    public class ApiKeyTokenCredentialTests
    {

        [Fact]
        public void Constructor_Build_Credential()
        {
            var expectedApiKey = "dapi1234";

            var options = new SqlWarehouseOptions { ApiKey = expectedApiKey };
            // Build an ApiKeyTokenCredential and check that it is not null
            var provider = new ApiKeyTokenCredential(options);

            // Assert
            Assert.NotNull(provider);
            Assert.Equal(expectedApiKey, provider.ApiKey);
        }

        [Fact]
        public async Task GetTokenAsync_AsExpected()
        {
            var expectedApiKey = "dapi1234";

            var options = new SqlWarehouseOptions { ApiKey = expectedApiKey };
            // Build an ApiKeyTokenCredential and check that it is not null
            var provider = new ApiKeyTokenCredential(options);


            // Assert
            var token = await provider.GetTokenAsync(new TokenRequestContext(), default);
            Assert.Equal(expectedApiKey, token.Token);
        }

        [Fact]
        public void GetToken_AsExpected()
        {
            var expectedApiKey = "dapi1234";

            var options = new SqlWarehouseOptions { ApiKey = expectedApiKey };
            // Build an ApiKeyTokenCredential and check that it is not null
            var provider = new ApiKeyTokenCredential(options);


            // Assert
            var token = provider.GetToken(new TokenRequestContext(), default);
            Assert.Equal(expectedApiKey, token.Token);
        }


        [Fact]
        public async Task Throw_CredentialUnavailableException_As_ApiKey_Not_Present()
        {
            var options = new SqlWarehouseOptions();
            // Build an ApiKeyTokenCredential and check that it is not null
            var provider = new ApiKeyTokenCredential(options);

            await Assert.ThrowsAsync<CredentialUnavailableException>(async () =>
            {
                await provider.GetTokenAsync(new TokenRequestContext(), default);
            });
        }


        [Fact]
        public void Throw_InvalidOperationException_As_Task_Not_Completed()
        {
            // mock the GetTokenAsync method to return a ValueTask that is not completed
            // use CallBase to call the base implementation of GetToken
            var mock = new Mock<ApiKeyTokenCredential>(Mock.Of<SqlWarehouseOptions>()) { CallBase = true };
            mock.Setup(x => x.GetTokenAsync(It.IsAny<TokenRequestContext>(), It.IsAny<CancellationToken>()))
                .Returns(new ValueTask<AccessToken>(Task.Delay(1000).ContinueWith(t => new AccessToken("test", DateTimeOffset.Now))));


            Assert.Throws<InvalidOperationException>(() =>
            {
                var t = mock.Object.GetToken(new TokenRequestContext(), default);
                Debug.WriteLine(t);
            });
        }
    }
}
