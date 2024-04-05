using Moq.Protected;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Databricks.Sql.Net.Tests
{
    public class Mockups
    {

        public static HttpClient GetHttpClientMock(string content)
        {
            return GetHttpClientSequenceMock([content]);
        }

        public static HttpClient GetHttpClientSequenceMock(params string[] contents)
        {
            var httpMessageHandlerMock = new Mock<HttpMessageHandler>();

            var seq = httpMessageHandlerMock.Protected().SetupSequence<Task<HttpResponseMessage>>("SendAsync",
                            ItExpr.IsAny<HttpRequestMessage>(),
                            ItExpr.IsAny<CancellationToken>());

            foreach (var content in contents)
                seq.ReturnsAsync(new HttpResponseMessage { StatusCode = HttpStatusCode.OK, Content = new StringContent(content) });

            seq.Throws(new InvalidOperationException());


            return new HttpClient(httpMessageHandlerMock.Object);
        }

        public const string DATABRICKS_JSON_MULTIPLE_CHUNKS_1 =
        @"{
          ""statement_id"": ""01eef342-dc09-1182-8df0-339cbf2cecd5"",
          ""status"": {
            ""state"": ""SUCCEEDED"",
            ""error"": null
          },
          ""manifest"": {
            ""format"": ""JSON_ARRAY"",
            ""schema"": {
                ""column_count"": 8,
                ""columns"": [
                {
                    ""name"": ""l_orderkey"",
                    ""type_text"": ""BIGINT"",
                    ""type_name"": ""LONG"",
                    ""position"": 0
                },
                {
                    ""name"": ""l_linenumber"",
                    ""type_text"": ""INT"",
                    ""type_name"": ""INT"",
                    ""position"": 1
                },
                {
                    ""name"": ""l_quantity"",
                    ""type_text"": ""DECIMAL(18,2)"",
                    ""type_name"": ""DECIMAL"",
                    ""position"": 2
                },
                {
                    ""name"": ""l_returnflag"",
                    ""type_text"": ""STRING"",
                    ""type_name"": ""STRING"",
                    ""position"": 3
                },
                {
                    ""name"": ""l_shipdate"",
                    ""type_text"": ""DATE"",
                    ""type_name"": ""DATE"",
                    ""position"": 4
                },
                {
                    ""name"": ""l_distance"",
                    ""type_text"": ""DOUBLE"",
                    ""type_name"": ""DOUBLE"",
                    ""position"": 5
                },
                {
                    ""name"": ""l_pickup_datetime"",
                    ""type_text"": ""TIMESTAMP"",
                    ""type_name"": ""TIMESTAMP"",
                    ""position"": 6
                },
                {
                    ""name"": ""l_isvalid"",
                    ""type_text"": ""BOOLEAN"",
                    ""type_name"": ""BOOLEAN"",
                    ""position"": 7
                }
                ]
            },
            ""total_row_count"": 200000,
            ""total_chunk_count"": 2,
            ""truncated"": true,
            ""chunks"": [
              {
                ""chunk_index"": 0,
                ""row_count"": 188416,
                ""row_offset"": 0
              },
              {
                ""chunk_index"": 1,
                ""row_count"": 11584,
                ""row_offset"": 188416
              }
            ]
          },
          ""result"": {
            ""row_count"": 188416,
            ""row_offset"": 0,
            ""chunk_index"": 0,
            ""next_chunk_index"": 1,
            ""next_chunk_internal_link"": ""/api/2.0/sql/statements/01eef342-dc09-1182-8df0-339cbf2cecd5/result/chunks/1"",
            ""data_array"": [
                [
                ""15997987"",
                ""4"",
                ""50.00"",
                ""A"",
                ""1992-02-12"",
                ""4.94"",
                ""2016-02-14T16:52:13.000Z"",
                ""true""
                ],
                [
                ""15997988"",
                ""1"",
                ""49.00"",
                ""A"",
                ""1994-05-31"",
                ""4.94"",
                ""2016-02-14T16:52:13.000Z"",
                ""true""
                ],
                [
                ""15997988"",
                ""2"",
                ""37.00"",
                ""A"",
                ""1994-05-24"",
                ""4.94"",
                ""2016-02-14T16:52:13.000Z"",
                ""true""
                ]
            ],
            ""external_links"": null
          }
        }
        ";

        public const string DATABRICKS_JSON_MULTIPLE_CHUNKS_2 =
        @"{
            ""row_count"": 11584,
            ""row_offset"": 188416,
            ""chunk_index"": 1,
            ""data_array"": [
                [
                ""15997987"",
                ""4"",
                ""50.00"",
                ""A"",
                ""1992-02-12"",
                ""4.94"",
                ""2016-02-14T16:52:13.000Z"",
                ""true""
                ],
                [
                ""15997988"",
                ""1"",
                ""49.00"",
                ""A"",
                ""1994-05-31"",
                ""4.94"",
                ""2016-02-14T16:52:13.000Z"",
                ""true""
                ],
                [
                ""15997988"",
                ""2"",
                ""37.00"",
                ""A"",
                ""1994-05-24"",
                ""4.94"",
                ""2016-02-14T16:52:13.000Z"",
                ""true""
                ]
            ],
            ""external_links"": null
        }
        ";

        public const string DATABRICKS_JSON_ARRAY_RESPONSE = @"
                {
                  ""statement_id"": ""01eef32c-380a-1ef3-b34f-b7fb2a9c637a"",
                  ""status"": {
                    ""state"": ""SUCCEEDED"",
                    ""error"": null
                  },
                  ""manifest"": {
                    ""format"": ""JSON_ARRAY"",
                    ""schema"": {
                      ""column_count"": 8,
                      ""columns"": [
                        {
                          ""name"": ""l_orderkey"",
                          ""type_text"": ""BIGINT"",
                          ""type_name"": ""LONG"",
                          ""position"": 0
                        },
                        {
                          ""name"": ""l_linenumber"",
                          ""type_text"": ""INT"",
                          ""type_name"": ""INT"",
                          ""position"": 1
                        },
                        {
                          ""name"": ""l_quantity"",
                          ""type_text"": ""DECIMAL(18,2)"",
                          ""type_name"": ""DECIMAL"",
                          ""position"": 2
                        },
                        {
                          ""name"": ""l_returnflag"",
                          ""type_text"": ""STRING"",
                          ""type_name"": ""STRING"",
                          ""position"": 3
                        },
                        {
                          ""name"": ""l_shipdate"",
                          ""type_text"": ""DATE"",
                          ""type_name"": ""DATE"",
                          ""position"": 4
                        },
                        {
                          ""name"": ""l_distance"",
                          ""type_text"": ""DOUBLE"",
                          ""type_name"": ""DOUBLE"",
                          ""position"": 5
                        },
                        {
                          ""name"": ""l_pickup_datetime"",
                          ""type_text"": ""TIMESTAMP"",
                          ""type_name"": ""TIMESTAMP"",
                          ""position"": 6
                        },
                        {
                          ""name"": ""l_isvalid"",
                          ""type_text"": ""BOOLEAN"",
                          ""type_name"": ""BOOLEAN"",
                          ""position"": 7
                        }
                      ]
                    },
                    ""total_row_count"": 3,
                    ""total_chunk_count"": 1,
                    ""truncated"": true,
                    ""chunks"": [
                      {
                        ""chunk_index"": 0,
                        ""row_count"": 3,
                        ""row_offset"": 0
                      }
                    ]
                  },
                  ""result"": {
                    ""row_count"": 3,
                    ""row_offset"": 0,
                    ""chunk_index"": 0,
                    ""next_chunk_index"": 0,
                    ""next_chunk_internal_link"": null,
                    ""data_array"": [
                      [
                        ""15997987"",
                        ""4"",
                        ""50.00"",
                        ""A"",
                        ""1992-02-12"",
                        ""4.94"",
                        ""2016-02-14T16:52:13.000Z"",
                        ""true""
                      ],
                      [
                        ""15997988"",
                        ""1"",
                        ""49.00"",
                        ""A"",
                        ""1994-05-31"",
                        ""4.94"",
                        ""2016-02-14T16:52:13.000Z"",
                        ""true""
                      ],
                      [
                        ""15997988"",
                        ""2"",
                        ""37.00"",
                        ""A"",
                        ""1994-05-24"",
                        ""4.94"",
                        ""2016-02-14T16:52:13.000Z"",
                        ""true""
                      ]
                    ],
                    ""external_links"": null
                  }
                }
                ";

    }
}
