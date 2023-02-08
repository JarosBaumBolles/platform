/*
	GCP BIGQUERY REST API WRAPPER functions
*/


const BASE_BQ_API_URL = "https://bigquery.googleapis.com/bigquery/v2/projects";

function getQueryRequest(requestQuery){
    let queryRequest = {
        query: requestQuery.query || "",
        location: requestQuery.location || "US",
        useLegacySql: requestQuery.useLegacySql || false,
        connectionProperties: requestQuery.connectionProperties || [],
        createSession: requestQuery.createSession || false,
        dryRun: requestQuery.dryRun || false,
        kind: requestQuery.kind || "bigquery#queryRequest",
        labels: requestQuery.labels || {},
        parameterMode: requestQuery.parameterMode || "",
        preserveNulls: requestQuery.preserveNulls || false,
        queryParameters: requestQuery.queryParameters || [],
        requestId: requestQuery.requestId || "",
        timeoutMs: requestQuery.timeoutMs || 0,
        useQueryCache: requestQuery.useQueryCache || true
    };

    if (!_.isUndefined(requestQuery.maxResults)){
        if (_.isNumber(requestQuery.maxResults)){
            queryRequest.maxResults = requestQuery.maxResults || 0;
        }
    }

    if (!_.isUndefined(requestQuery.maximumBytesBilled)){
        if (_.isNumber(requestQuery.maxResults)){
            queryRequest.maximumBytesBilled = requestQuery.maximumBytesBilled || 0;
        }
    }

    if (!_.isUndefined(requestQuery.defaultDataset) && _.isObject(requestQuery.defaultDataset)){
        if(_.has(requestQuery.defaultDataset, "datasetId")){
            if(_.isString(requestQuery.defaultDataset.datasetId)){
                queryRequest.maximumBytesBilled = requestQuery.maximumBytesBilled || 0;
            }
        }
    }

    return queryRequest;

}

async function gcpQueryWrapper(
    project,
    requestQuery,
    options,
){
    
    let url = new URL(`${BASE_BQ_API_URL}/${project}/queries`),
        requestOptions = {
            method: 'POST',
            headers: new Headers(),
            redirect: 'follow',
            body: JSON.stringify(getQueryRequest(requestQuery))
        },
        acsess_token=getAccessToken();

    options = options || {};
    requestQuery = requestQuery || {}
    
    if (_.isEmpty(project)){
        throw Error(`[BigQuery]. The project is not defined.`);
    }

    if (_.isEmpty(requestQuery) || _.isEmpty(requestQuery.query)){
        throw Error(`The query request body or query is empty.`);
    }

    url.search = new URLSearchParams(
        {
            quotaUser: options.quotaUser || '',
            fields: options.fields || "",
            userIp: options.userIp || "",
            prettyPrint: options.prettyPrint || true,
            alt: options.alt || 'json'
        }
    ).toString();

    requestOptions.headers.append(
        'Authorization', 
        `Bearer ${acsess_token}`
    );

    console.debug("URL", url.toString());
    console.debug("BigQuery requestBody", requestOptions.body);
    console.debug("requestOptions", requestOptions.headers.values());
    return await fetch(url, requestOptions);
}


async function gcpQuery(
    project,
    requestQuery,
    options,
){
    return gcpQueryWrapper(
        project,
        requestQuery,
        options,
    ).then(
        async (resp) => {
			if (resp.ok){
				return resp.json();
			}

			let errBlob = await resp.blob(),
				errMsg = await errBlob.text();

			throw Error(`${resp.status} - ${errMsg}`);	
        }
    ).then(
        data => {
            let fields = _.map(data.schema.fields, value => value.name);
            return _.map(
                data.rows, 
                value => {
                    return _.zipObject(
                        fields, 
                        _.map(value.f, "v")
                    )
                }
            )
        }
    ).catch(
        err => {
            throw Error(`Recived Error '${err.message}' during query running`)
        }
    )
}