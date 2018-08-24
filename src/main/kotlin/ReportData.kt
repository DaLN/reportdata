import com.amazonaws.services.athena.AmazonAthena
import com.amazonaws.services.athena.model.*
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent
import com.google.gson.Gson




class ReportData: RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private val ATHENA_OUTPUT_BUCKET: String = "s3://athena-output-bucket-from-lambda"
    private val ATHENA_QUERY: String = "SELECT * FROM \"23andme_data\".\"snplistbucket\" WHERE reference IN (SELECT rsid FROM report_23andme_trimmed) AND riskallele IN (SELECT genotype FROM report_23andme_trimmed);"
    private val ATHENA_DEFAULT_DATABASE: String = "23andme_data"

    /**
     * Handles a Lambda Function request
     * @param input The Lambda Function input
     * @param context The Lambda execution environment context object.
     * @return The Lambda Function output
     */
    override fun handleRequest(input: APIGatewayProxyRequestEvent, context: Context) : APIGatewayProxyResponseEvent {
        // Build an AmazonAthena client
        val factory = AthenaClientFactory()
        val client = factory.createClient()

        val queryExecutionId = submitAthenaQuery(client)

        waitForQueryToComplete(client, queryExecutionId)

        val gson = Gson()
        val reportLineList = processResultRows(client, queryExecutionId)
       /* for (row in getQueryResultsResult.resultSet.rows) {
            row.data
        } */
        val response:APIGatewayProxyResponseEvent = APIGatewayProxyResponseEvent()
                .withBody(gson.toJson(reportLineList))
                .withStatusCode(200)
        return response
    }

    /**
     * Submits a sample query to Athena and returns the execution ID of the query.
     */
    private fun submitAthenaQuery(client: AmazonAthena): String {
        // The QueryExecutionContext allows us to set the Database.
        val queryExecutionContext = QueryExecutionContext().withDatabase(ATHENA_DEFAULT_DATABASE)

        // The result configuration specifies where the results of the query should go in S3 and encryption options
        val resultConfiguration = ResultConfiguration()
                // You can provide encryption options for the output that is written.
                // .withEncryptionConfiguration(encryptionConfiguration)
                .withOutputLocation(ATHENA_OUTPUT_BUCKET)

        // Create the StartQueryExecutionRequest to send to Athena which will start the query.
        val startQueryExecutionRequest = StartQueryExecutionRequest()
                .withQueryString(ATHENA_QUERY)
                .withQueryExecutionContext(queryExecutionContext)
                .withResultConfiguration(resultConfiguration)

        val startQueryExecutionResult = client.startQueryExecution(startQueryExecutionRequest)
        return startQueryExecutionResult.queryExecutionId
    }

    /**
     * Wait for an Athena query to complete, fail or to be cancelled. This is done by polling Athena over an
     * interval of time. If a query fails or is cancelled, then it will throw an exception.
     */

    @Throws(InterruptedException::class)
    private fun waitForQueryToComplete(client: AmazonAthena, queryExecutionId: String) {
        val getQueryExecutionRequest = GetQueryExecutionRequest()
                .withQueryExecutionId(queryExecutionId)

        var getQueryExecutionResult: GetQueryExecutionResult? = null
        var isQueryStillRunning = true
        while (isQueryStillRunning) {
            getQueryExecutionResult = client.getQueryExecution(getQueryExecutionRequest)
            val queryState = getQueryExecutionResult!!.queryExecution.status.state
            if (queryState == QueryExecutionState.FAILED.toString()) {
                throw RuntimeException("Query Failed to run with Error Message: " + getQueryExecutionResult.queryExecution.status.stateChangeReason)
            } else if (queryState == QueryExecutionState.CANCELLED.toString()) {
                throw RuntimeException("Query was cancelled.")
            } else if (queryState == QueryExecutionState.SUCCEEDED.toString()) {
                isQueryStillRunning = false
            } else {
                // Sleep an amount of time before retrying again.
                Thread.sleep(1000)
            }
            println("Current Status is: $queryState")
        }
    }

    /**
     * This code calls Athena and retrieves the results of a query.
     * The query must be in a completed state before the results can be retrieved and
     * paginated. The first row of results are the column headers.
     */
    private fun processResultRows(client: AmazonAthena, queryExecutionId: String): List<ReportLine> {
        val getQueryResultsRequest = GetQueryResultsRequest()
                // Max Results can be set but if its not set,
                // it will choose the maximum page size
                // As of the writing of this code, the maximum value is 1000
                // .withMaxResults(1000)
                .withQueryExecutionId(queryExecutionId)

        var queryResults = client.getQueryResults(getQueryResultsRequest)
        val columnInfoList = queryResults.resultSet.resultSetMetadata.columnInfo
        val reportLineList = mutableListOf<ReportLine>()
        while (true) {
            val results = queryResults.resultSet.rows
            for (row in results) {
                // Process the row. The first row of the first page holds the column names.
                reportLineList.add(processRow(row, columnInfoList))
            }
            // If nextToken is null, there are no more pages to read. Break out of the loop.
            if (queryResults.nextToken == null) {
                break
            }
            queryResults = client.getQueryResults(
                    getQueryResultsRequest.withNextToken(queryResults.nextToken))
        }

        return reportLineList.toList()
    }

    private fun processRow(row: Row, columnInfoList: List<ColumnInfo>): ReportLine {
        var gene:String? = null
        var reference:String? = null
        var riskallele:String? = null
        var maf:String? = null
        var lifestylefactor:String? = null
        var nutrienteffects:String? = null
        var geneeffects:String? = null
        var condition:String? = null

        for (i in columnInfoList.indices) {
            when (columnInfoList[i].type) {
                "varchar" -> {
                    // Convert and Process as String
                    when (columnInfoList[i].label) {
                        "gene" -> {
                            gene = row.data.get(i).varCharValue
                        }
                        "reference" -> {
                            reference = row.data.get(i).varCharValue
                        }
                        "riskallele" -> {
                            riskallele = row.data.get(i).varCharValue
                        }
                        "maf" -> {
                            maf = row.data.get(i).varCharValue
                        }
                        "lifestylefactor" -> {
                            lifestylefactor = row.data.get(i).varCharValue
                        }
                        "nutrienteffects" -> {
                            nutrienteffects = row.data.get(i).varCharValue
                        }
                        "geneeffects" -> {
                            geneeffects = row.data.get(i).varCharValue
                        }
                        "condition" -> {
                            condition = row.data.get(i).varCharValue
                        }
                        else -> throw RuntimeException("Unexpected Label: " + columnInfoList[i].label)
                    }
                }
                /*"tinyint" -> {
                    // Convert and Process as tinyint
                }
                "smallint" -> {
                    // Convert and Process as smallint
                }
                "integer" -> {
                    // Convert and Process as integer
                }
                "bigint" -> {
                    // Convert and Process as bigint
                }
                "double" -> {
                    // Convert and Process as double
                }
                "boolean" -> {
                    // Convert and Process as boolean
                }
                "date" -> {
                    // Convert and Process as date
                }
                "timestamp" -> {
                    // Convert and Process as timestamp
                }*/
                else -> throw RuntimeException("Unexpected Type: " + columnInfoList[i].type)
            }
        }
        return ReportLine(gene!!, reference!!, riskallele!!, maf!!, lifestylefactor!!,
                nutrienteffects!!, geneeffects!!, condition!!)
    }
}
