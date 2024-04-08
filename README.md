# .Net connector to Databricks SQL Warehouse.


## Installation

**_NOT YET PUBLISHED. USED THE SOURCE CODE UNTIL IT'S PUBLISHED_**

```bash
dotnet add package Databricks.SqlWareHouse
```

## Usage

The `SqlWarehouseConnection` class is the main entry point for the library. It provides methods to execute SQL queries and retrieve the results.


``` csharp
using Databricks.SqlWareHouse;

var options = new SqlWarehouseConnectionOptions
{
	Host = "https://<databricks-instance>.azuredatabricks.net",
	ApiKey = "<databricks-token>",
	WarehouseId = "<warehouse-id>",
	Catalog = "<catalog>",
	Schema = "<schema>"
};

var connection = new SqlWarehouseConnection(options);
```

The `SqlWarehouseConnection` constructor has several options you can set:

- `HttpClient` : Use your own `HttpClient` instance.
- `Policy` : Use a custom retry policy. (inspired from Polly)
- `DefaultAzureCredentialOptions` : If you want to use Azure Identity to authenticate, and specify the options.
- `TokenCredential` : If you want to use your own TokenCredential to authenticate.

Once connected, you can use some out of the box methods to interact with the Databricks SQL Warehouse, from your `SqlWarehouseConnection` instance.

You can also use the `SqlWarehouseConnection` to create a `SqlWarehouseCommand` instance, which you can use to execute SQL queries (see relted section).

### ASP.NET Dependency Injection

If you are using the connector within an ASP.NET application you can use the extension method to inject an `SqlWarehouseConnection` instance:

``` csharp
builder.Services.AddSqlWarehouse(builder.Configuration);
```

Configuration is expected to be in the `appsettings.json` file, using a section called `Databricks` (that you can customize in the `AddSqlWarehouse` method) :

``` json
{
  "Databricks": {
    "Host": "https://<databricks-instance>.azuredatabricks.net",
    "ApiKey": "<databricks-token>",
    "WarehouseId": "<warehouse-id>",
    "Catalog": "samples",
    "Schema": "tpch",
    "WaitTimeout":10,
    "TenantId": "<tenant-id>"
  }
}
```

## SqlWarehouseConnection methods

These are the methods you can use to interact with the Databricks SQL Warehouse, directly from your `SqlWarehouseConnection` instance.

### Get Warehouses

Get an array of all the warehouses in the Databricks instance.

``` csharp
var tables = await connection.GetSqlWarehousesAsync();
```

``` json
[
  {
    "id": "xxx-xxxx-xxxxx-xxxx",
    "name": "serverless_warehouse",
    "type": "databricks_internal",
    "warehouseId": "123456bade9875",
    "syntax": "sql",
    "paused": 0,
    "pausReason": null,
    "supportsAutoLimit": true,
    "viewOnly": false
  }
]
````

### GetSchemaAsync

Describe the schema, with all the tables, you are connected to.

``` csharp
var schema = await connection.GetSchemaAsync();
```

``` json
{
  "name": "tpch",
  "catalog": "samples",
  "comment": "Sample database tpch",
  "location": "",
  "tables": [
    {
      "name": "customer",
      "schema": "tpch",
      "catalog": "samples",
      "isTemporary": false,
      "columns": null,
      "properties": null
    },
  ]
}
````

### GetTableAsync

Get the details of a table, including columns and table properties.

``` csharp
var table = await connection.GetTableAsync("table-name");
```

``` json
{
  "name": "customer",
  "schema": "tpch",
  "catalog": "samples",
  "isTemporary": false,
  "columns": [    
    {"name": "c_custkey",  "dataType": "bigint" },
    {"name": "c_name","dataType": "string" },
    {"name": "c_address", "dataType": "string" },
    {"name": "c_nationkey", "dataType": "bigint" },
    {"name": "c_phone", "dataType": "string"  }
  ],
  "properties": {
    "Created Time": "Mon Apr 08 08:09:47 UTC 2024",
    "Last Access": "UNKNOWN",
    "Created By": "Spark ",
    "Type": "EXTERNAL",
    "Location": "dbfs:/xxxxxxxxx/customer",
    "Provider": "delta",
    "Table Properties": "[delta.checkpoint.writeStatsAsJson=false,delta.checkpoint.writeStatsAsStruct=true,delta.minReaderVersion=1,delta.minWriterVersion=2]"
  }
}
```

### GetSqlWarehousesQueriesAsync

Get an array of all the queries saved in your workspace.

``` csharp
var queries = await connection.GetSqlWarehousesQueriesAsync();
```

``` json
{
  "count": 2,
  "page": 1,
  "pageSize": 25,
  "results": [
    {
      "id": "xxxxx-xxxx-xxxx-xxxxx-xxxxx",
      "parent": null,
      "latestQueryDataId": null,
      "name": "Trips",
      "description": null,
      "query": "Select * from samples.nyctaxi.trips where dropoff_zip = {{ zip }} and trip_distance < {{ dist }}",
      "queryHash": null,
      "isArchived": null,
      "isDraft": false,
      "updatedAt": "2024-04-05T17:16:19Z",
      "createdAt": "2024-04-05T16:53:02Z",
      "dataSourceId": "xxxxx-xxxx-xxxx-xxxxx-xxxxx",
      "options": {
        "movedToTrashAt": null,
        "parameters": [
          {
            "title": "Zip",
            "name": "zip",
            "type": "number",
            "enumOptions": null,
            "queryId": null,
            "multiValuesOptions": null,
            "value": 10003
          },
          {
            "title": "dist",
            "name": "dist",
            "type": "number",
            "enumOptions": null,
            "queryId": null,
            "multiValuesOptions": null,
            "value": 1
          }
        ]
      },
      "tags": [],
      "isSafe": true,
      "userId": 88888888888888888,
      "lastModifiedById": null,
      "visualizations": null,
      "isFavorite": false,
      "user": {
        "id": 888888888888888,
        "name": "John Doe",
        "email": "johndoe@contoso.com"
      },
      "lastModifiedBy": null,
      "canEdit": null,
      "permissionTier": null,
      "runAsRole": "owner"
    }
   ]
}
```

### GetSqlWarehousesQueriesHystoryAsync

Get history of all the queries executed in your workspace.
You can also filter the results by passing:

- `maxResults` : The maximum number of results to return.
- `pageToken` : The token to use to get the next page of results.
- `includeMetrics` : Whether to include metrics in the response.

``` csharp
var queries = await connection.GetSqlWarehousesQueriesHystoryAsync();
```

``` json
{
  "nextPageToken": "xxxxxxxxxxxxxxxxxxxxo3wEYZA==",
  "hasNextPage": true,
  "res": [
    {
      "queryId": "xxxxxx-xxxx-xxxx-xxxxxx",
      "status": "FINISHED",
      "queryText": "SELECT *  FROM samples.tpch.customer",
      "queryStartTimeMs": 1712523190605,
      "executionEndTimeMs": 1712523201957,
      "queryEndTimeMs": 1712523201957,
      "userId": 8888888888888888,
      "userName": "johndoe@contoso.com",
      "sparkUiUrl": "https://xxxxxx",
      "endpointId": "88888888888888888",
      "warehouseId": "88888888888888888",
      "lookupKey": "Cixxxxxxxxxxxxxxxxxxxxxxxxx",
      "errorMessage": null,
      "rowsProduced": 0,
      "canSubscribeToLiveQuery": null,
      "metrics": {},
      "isFinal": true,
      "channelUsed": {},
      "duration": 11352,
      "executedAsUserId": 88888888888888,
      "executedAsUserName": "johndoe@contoso.com",
      "plansState": "EXISTS",
      "statementType": "SHOW"
    }
  ]
}
```

# GetSqlWarehousePermissionsAsync

Get the permissions of a warehouse.

``` csharp
var permissions = await connection.GetSqlWarehousePermissionsAsync("warehouse-id");
```

``` json
{
  "objectId": "/sql/warehouses/1245678azerty",
  "objectType": "warehouses",
  "accessControlList": [
    {
      "userName": "johndoe@contoso.com",
      "groupName": null,
      "servicePrincipalName": null,
      "displayName": "John Doe",
      "allPermissions": [
        {
          "permissionLevel": "IS_OWNER",
          "inherited": false,
          "inheritedFromObject": null
        }
      ]
    },
    {
      "userName": null,
      "groupName": "admins",
      "servicePrincipalName": null,
      "displayName": null,
      "allPermissions": [
        {
          "permissionLevel": "CAN_MANAGE",
          "inherited": true,
          "inheritedFromObject": [
            "/sql/warehouses/"
          ]
        }
      ]
    }
  ]
}
```

## Execute SQL queries using SqlWarehouseCommand

The `SqlWarehouseCommand` class is used to execute SQL queries on the Databricks SQL Warehouse.

### Methods

Each method getting results from the Databricks SQL Warehouse allows you to specify the maximum number of rows to return.

Your main entry points are the `LoadJsonAsync` and `LoadJson<T>` methods, which returns either a JSON array or a list of objects of type `T`.

The two others methods, `GetJsonObjectsAsync` and `ExecuteAsync` are used internally by the two main methods.

- `GetJsonObjectsAsync`: A special asynchronous method is available to execute a SQL query and return an `IAsyncEnumerable<JsonObject>`. This method is used by all the other methods internally.
- `ExecuteAsync` : Execute a SQL query and return the result as a `DbricksResult` object. This method is using `GetJsonObjectsAsync` and is internally used by `LoadJsonAsync` and `LoadJson<T>`.
- `LoadJsonAsync` : Execute a SQL Query and return the result as a `JsonArray` object.
- `LoadJson<T>` : Execute a SQL Query and return the result as a list of objects of type `T`.


``` csharp
var command = new SqlWarehouseCommand(connection, "SELECT l_orderkey, l_extendedprice, l_shipdate FROM lineitem");

var dbricksResult = await command.LoadJsonAsync(3);
```

``` json
[
  {
	"l_orderkey": "15997987",
	"l_extendedprice": "66516.00",
	"l_shipdate": "1992-02-12"
  },
  {
	"l_orderkey": "15997988",
	"l_extendedprice": "53460.96",
	"l_shipdate": "1994-05-31"
  },
  {
	"l_orderkey": "15997988",
	"l_extendedprice": "47738.88",
	"l_shipdate": "1994-05-24"
  }
]
```

Using the `ExecuteAsync` method, you can get the result as it is coming out of Databricks, wrapped in a `DbricksResult` object, which contains the result of the query, the schema, and the status of the query.

``` csharp
var command = new SqlWarehouseCommand(connection, "SELECT l_orderkey, l_extendedprice, l_shipdate FROM lineitem");

var dbricksResult = await command.ExecuteAsync(3);
```

``` json
{
  "statementId": "xxxxx-xxxxxx-xxxxx-xxxxx",
  "status": {
    "state": "SUCCEEDED",
    "error": null
  },
  "manifest": {
    "format": "JSON_ARRAY",
    "schema": {
      "columnCount": 3,
      "columns": [
        {
          "name": "l_orderkey",
          "typeText": "BIGINT",
          "typeName": "LONG",
          "position": 0
        },
        {
          "name": "l_extendedprice",
          "typeText": "DECIMAL(18,2)",
          "typeName": "DECIMAL",
          "position": 1
        },
        {
          "name": "l_shipdate",
          "typeText": "DATE",
          "typeName": "DATE",
          "position": 2
        }
      ]
    },
    "totalRowCount": 3,
    "totalChunkCount": 1,
    "truncated": true,
    "chunks": [
      {
        "chunkIndex": 0,
        "rowCount": 3,
        "rowOffset": 0
      }
    ]
  },
  "result": {
    "rowCount": 3,
    "rowOffset": 0,
    "chunkIndex": 0,
    "nextChunkIndex": null,
    "nextChunkInternalLink": null,
    "dataArray": [
      [
        "15997987",
        "66516.00",
        "1992-02-12"
      ],
      [
        "15997988",
        "53460.96",
        "1994-05-31"
      ],
      [
        "15997988",
        "47738.88",
        "1994-05-24"
      ]
    ],
    "externalLinks": null
  }
}
```

### Command Parameters

You can specify parameters in your SQL query, using the `AddParameter` method.

``` csharp
var command = new SqlWarehouseCommand(connection, "SELECT * FROM samples.tpch.customer WHERE c_int = :c_int and c_date = :c_date");

command.Parameters.AddInt("c_int", 1);
command.Parameters.AddDate("c_date", date);
```

### Execution Progress

You can get the progress of the execution of your query, using the `GetProgressAsync` method.

``` csharp
var progress = new Progress<StatementProgress>();
progress.ProgressChanged += (sender, e) => Debug.WriteLine(e);

var command = new SqlWarehouseCommand(connection, "SELECT l_orderkey, l_extendedprice, l_shipdate FROM lineitem");
var json = await command.LoadJsonAsync(count, progress);
```

## Authentication


Authentication is done using a **Databricks token** or using the **Azure Identity** provider.


You can retrieve the current token using the `GetTokenAsync` method.

``` csharp
var token = await connection.GetTokenAsync();
```

### Using a Databricks token

Just configure your API Key from your `SqlWarehouseConnectionOptions` instance, or use the configuration section:

``` json
{
  "Databricks": {
    "Host": "https://<databricks-instance>.azuredatabricks.net",
    "ApiKey": "<databricks-token>",
    "WarehouseId": "<warehouse-id>",
    "Catalog": "samples",
    "Schema": "tpch",
    "WaitTimeout":10,
    "TenantId": "<tenant-id>"
  }
}
```


### Using Azure Identity



You can use [Azure Identity](https://learn.microsoft.com/en-us/dotnet/api/overview/azure/identity-readme?view=azure-dotnet) to authenticate your connection with the Databricks SQL Warehouse.

Internally, the library uses the `DefaultAzureCredential` class from the `Azure.Identity` framework to authenticate your connection with the Databricks SQL Warehouse.
If you want to use a managed identity, you can set the `ManagedIdentityClientId` value in your options :

``` json
{
  "Host": "https://<databricks-instance>.azuredatabricks.net",
  "WarehouseId": "<warehouse-id>",
  "Catalog": "<catalog>",
  "Schema": "<schema>",
  "ManagedIdentityClientId": "<managed-identity-client-id>"
}
```

You can also use the `DefaultAzureCredentialOptions` class to specify the options for the `DefaultAzureCredential` class.

``` csharp
var azureAuthOptions = new DefaultAzureCredentialOptions
    {
        ExcludeInteractiveBrowserCredential = false,
        ManagedIdentityClientId = options.ManagedIdentityClientId,
        TenantId = options.TenantId,
    };

var connection = new SqlWarehouseConnection(options, azureAuthOptions);
```

### Using a custom authentication method, with TokenCredential

If you want to use your own `TokenCredential` to authenticate, you can create your own authentication class, inhereting from `TokenCredential`, and use it from your `SqlWarehouseConnection`:

Useful if you want to use your own authentication logic:

``` csharp
public class MyTokenCredential : TokenCredential
{
	public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken)
	{
		// Your authentication logic here
	}
}

var tokenCredential = new MyTokenCredential();

var connection = new SqlWarehouseConnection(options, customTokenCredential: tokenCredential);
```


