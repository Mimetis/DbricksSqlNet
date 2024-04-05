using Azure.Core;
using Databricks.Sql.Net.Authentication;
using Databricks.Sql.Net.Options;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Databricks.Sql.Net.Tests
{
    public class AuthenticationProviderTests
    {

        [Fact]
        public void Constructor_Build_Credential()
        {
            var expectedToken = "eyjxxxx";
            var expectedLifetime = DateTimeOffset.Now.AddMinutes(5);

            // create a Mock of TokenCredential that returns a token
            var mockTokenCredential = new Mock<TokenCredential>();
            mockTokenCredential.Setup(
                x => x.GetTokenAsync(It.IsAny<TokenRequestContext>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new AccessToken(expectedToken, expectedLifetime));

           var provider = new AuthenticationProvider(mockTokenCredential.Object, Mock.Of<SqlWarehouseOptions>());

            // Assert
            Assert.NotNull(provider);
            Assert.NotNull(provider.Credential);
        }

        [Fact]
        public async Task GetTokenAsync_AsExpected()
        {
            var expectedToken = "eyjxxxx";
            var expectedLifetime = DateTimeOffset.Now.AddMinutes(5);

            // create a Mock of TokenCredential that returns a token
            var mockTokenCredential = new Mock<TokenCredential>();
            mockTokenCredential.Setup(
                x => x.GetTokenAsync(It.IsAny<TokenRequestContext>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new AccessToken(expectedToken, expectedLifetime));

            var provider = new AuthenticationProvider(mockTokenCredential.Object, Mock.Of<SqlWarehouseOptions>());

            // Assert
            var token = await provider.GetTokenAsync();
            Assert.NotNull(token);
            Assert.Equal(expectedToken, token);
        }
    }
}
