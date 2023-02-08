/*
	GCP-specific functions
*/

const CLIENT_ID = "858895469875-onqub4gft68m5tcl2dd20copij3oqv26.apps.googleusercontent.com",
	DISCOVERY_DOCS = [
		'https://www.googleapis.com/discovery/v1/apis/storage/v1/rest',
		'https://bigquery.googleapis.com/discovery/v1/apis/bigquery/v2/rest',
		'https://www.googleapis.com/discovery/v1/apis/admin/directory_v1/rest',
		'https://cloudidentity.googleapis.com/$discovery/rest?version=v1'
	],

	SCOPES = (
		'https://www.googleapis.com/auth/bigquery.readonly ' + 
		'https://www.googleapis.com/auth/devstorage.read_write ' + 
		'https://www.googleapis.com/auth/admin.directory.group.readonly ' +
		'https://www.googleapis.com/auth/admin.directory.user.readonly'
	);

let CLIENT_TOKEN,
	GAPI_INITED = false,
    GIS_INITED = false;

function handleGapiLoad() {
	initClient();
}

async function uploadReader(fileData, i, bucket, path, batchTime) {
	const fileName = `${path}-${i}-${batchTime}`;
	var reader = new FileReader();
	console.info(`Start file "${fileData.name}" uploading.`);
	reader.onload = async function(progressEvt) {
		if (progressEvt.currentTarget.readyState == progressEvt.currentTarget.DONE){
			let raw = progressEvt.currentTarget.result;
			if (raw.length != 0){
				let res = generateMultyPartFileBody(fileName, raw, fileData.type);
				await insertMultiPartObject(
					bucket,
					fileName,
					res.body,
					res.boundary
				)
			}else{
				console.warn(`The file "${fileData.name}" is empty. Skipping.`);
			}
		} else if (progressEvt.currentTarget.readyState == progressEvt.currentTarget.LOADING){
			console.warn(`The file "${fileData.name}" reading still in progress.`);
		} else if (progressEvt.currentTarget.readyState == progressEvt.currentTarget.LOADING){
			console.warn(`The file "${fileData.name}" is empty. Skipping.`);
		}

	};
	reader.readAsBinaryString(fileData);
}

async function uploadToStorageBucket(uploadEvent) {
	const res = await extendedAttempt(
		async (event) => {
			const bucket = event.target.getAttribute("data-bucket"),
				path = event.target.getAttribute("data-path"),
				batchTime = new Date().toISOString().slice(0, 19);

			let selectedFiles = Array.from(event.target.files);
			const promises = await selectedFiles.map(
				async (uploadFile, flIndex) => {
					uploadReader(
						uploadFile, 
						flIndex, 
						bucket, 
						path, 
						batchTime
					);
				},				
			);
			await Promise.all(promises);
			console.info(`Finished processing meters configuration files of ${bucket}`);
		},
		uploadEvent
	).then(
		() => {
			console.log("Completed file uploading")
		}
	).catch(err => {
		const st = uploadEvent.target.getAttribute("data-status");
		$(`#${st}`).html(`Failed to schedule: ${err.message}`);
		console.log(`Failed to schedule: ${err.message}`);
	});
}

async function listFiles(bucket, prefix, startOffset, endOffset) {
	return gcpListObjects(
		bucket,
		prefix, 
		startOffset, 
		endOffset
	).then(
		async(response) => {
			if (response.ok || response.status == 200){
				return response.json();
			}
			let errBlob = await response.blob(),
				errMsg = await errBlob.text();

			throw Error(`${response.status} - ${errMsg}`);	
		}
	).then(
		data => data.items || []
	).catch( err => {
		throw Error(`gs://${bucket}/${prefix} error: ${err.message}`)
	});
}

async function getObject(bucket, path) {
	return gcpGetObject(bucket, path).then(
		async function(res) {
			if (res.ok){
				return res.blob();
			}

			let errBlob = await res.blob(),
				errMsg = await errBlob.text();

			throw Error(`${res.status} - ${errMsg}`);		
		}
	).then(
		data => data.text()
	).catch( err => {
		throw Error(`Error fetching gs://${bucket}/${path} error: ${err.message}`)
	});
}

async function download_from_bucket(event) {
	event.preventDefault();
	const bucket = $(event.currentTarget).data("bucket"),
		path = $(event.currentTarget).data("path");
	gcpGetObject(bucket, path).then(
		async function(res){
			if (res.ok){
				return res.blob();
			}

			let errBlob = await res.blob(),
				errMsg = await errBlob.text();

			throw Error(`${res.status} - ${errMsg}`);		
		}
	).then(
		async (data) => {
			// It is necessary to create a new blob object with mime-type 
			// explicitly set otherwise only Chrome works like it should
			const blb_txt = await data.text(),
				blob = new Blob(
				[blb_txt], 
				{
					type: 'application/xml'
				}
			);

			if (typeof window.navigator.msSaveBlob !== 'undefined') {
				// IE doesn't allow using a blob object directly as link href.
				// Workaround for "HTML7007: One or more blob URLs were
				// revoked by closing the blob for which they were created.
				// These URLs will no longer resolve as the data backing
				// the URL has been freed."
				window.navigator.msSaveBlob(blob, path);
				return;
			}

			// Other browsers
			// Create a link pointing to the ObjectURL containing the blob
			const blobURL = window.URL.createObjectURL(blob);
			const tempLink = document.createElement('a');
			// tempLink.style.display = 'none';
			tempLink.href = blobURL;
			tempLink.setAttribute('download', path);

			// Safari thinks _blank anchor are pop ups. We only want to set 
			// _blank target if the browser does not support the HTML5 
			// download attribute. This allows you to download files in 
			// desktop safari if pop up blocking is enabled.
			if (typeof tempLink.download === 'undefined') {
				tempLink.setAttribute('target', '_blank');
			}
			document.body.appendChild(tempLink);
			tempLink.click();
			// document.body.removeChild(tempLink);
			// setTimeout(() => {
			// 	// For Firefox it is necessary to delay revoking the ObjectURL
			// 	window.URL.revokeObjectURL(blobURL);
			// 	// document.body.removeChild(tempLink);
			// }, 100);		
		}
	).catch( err => {
		throw Error(`Error fetching gs://${bucket}/${path} error: ${err.message}`)
	});
}

function query(projectId, query) {
	return gcpQuery(
		projectId, 
		{
			query: query,
			location: "US",
			timeoutMs:100000,
		}
	).then(
		result => {return result}
	).catch(
		err => {
			console.error(`Cannor run the query "${query}" due to the ${err}`);
			throw Error(err);
		}
	).catch(
		() => []
	);
}
