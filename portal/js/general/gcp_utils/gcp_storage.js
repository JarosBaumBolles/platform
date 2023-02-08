/*
	GCP-STORAGE REST API WRAPPER functions
*/

const BASE_API_URL = "https://storage.googleapis.com/storage/v1/b",
    BASE_UPLOAD_API_URL = "https://storage.googleapis.com/upload/storage/v1/b";

function getAccessToken(){
    var gAuth2 = gapi.auth2.getAuthInstance(),
    user = gAuth2.currentUser.get();
    
    return user.getAuthResponse(true).access_token;
}

async function gcpListObjects(
    bucket, 
    prefix,
    startOffset,
    endOffset, 
    alt
){
    var url = new URL(`${BASE_API_URL}/${bucket}/o`),
        requestOptions = {
            method: 'GET',
            headers: new Headers(),
            redirect: 'follow'
        },
        acsess_token=getAccessToken();

    url.search = new URLSearchParams(
        {
            prefix: prefix,
            startOffset: startOffset,
            endOffset: endOffset,
            alt: alt || 'json'
        }
    ).toString();

    requestOptions.headers.append(
        'Authorization', 
        `Bearer ${acsess_token}`
    );

    return await fetch(url, requestOptions);
}

async function gcpGetObject(
    bucket, 
    prefix
){
    var requestOptions = {
            method: 'GET',
            headers: new Headers(),
            redirect: 'follow'
        },
        acsess_token=getAccessToken(),
        url,
        encodedPrefix;
    requestOptions.headers.append(
        'Authorization', 
        `Bearer ${acsess_token}`
    );
    encodedPrefix = encodeURIComponent(prefix);
    url = new URL(`${BASE_API_URL}/${bucket}/o/${encodedPrefix}`);
    url.search = new URLSearchParams({alt:'media'}).toString();
    return await fetch(url, requestOptions);
}


async function insertMultiPartObject(
    bucket,
    fileName,
    fileBody,
    boundary,
){
    // In accordance with documentation https://cloud.google.com/storage/docs/json_api/v1/objects/insert
    // Stores a new object and metadata. The uploaded object replaces any existing object with the same name. 
 
    // This method accepts uploaded object data with the following characteristics:
    // Maximum file size: 5 TiB
    // Accepted Media MIME types: */*
    // This method generally requires the following headers be included in a request:
    // Content-Length
    // Content-Type
    // The authenticated user must have sufficient permission to use this method.
    // Note: Metadata-only requests are not allowed. To change an object's metadata, 
    // use either the update or patch methods.

    var url = new URL(`${BASE_UPLOAD_API_URL}/${bucket}/o`),
        requestOptions = {
            method: 'POST',
            headers: new Headers(),
            body: fileBody,
            redirect: 'follow',
        },
        acsess_token=getAccessToken();

        requestOptions.headers.append('Authorization', `Bearer ${acsess_token}`);
        requestOptions.headers.append(
            'Content-Type', 
            `multipart/related; boundary="${boundary}"`
        );  
        
        url.search = unescape(
            new URLSearchParams(
                {
                    uploadType: 'multipart',
                    name: fileName
                }
            ).toString()
        );        
        
        console.log("URL", url.toString());
        console.log("requestOptions", requestOptions.headers.values());
        return await fetch(url, requestOptions);
}
