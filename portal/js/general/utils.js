function getBasename(prevname) {
	return prevname.replace(/^(.*[/\\])?/, '').replace(/(\.[^.]*)$/, '');
};

function getUserName(){
    var gAuth2 = gapi.auth2.getAuthInstance(),
        user;
    if (gAuth2.isSignedIn.get()){
        user = gAuth2.currentUser.get();
        return user.getBasicProfile().getName()
    }
    return 'Unknown'
}  

async function extendedAttempt(func, ...args){
	return new Promise(
		async function (resolve, reject){
			try{
				let result = await func(...args);
				resolve(result);
			} catch(err){
				reject(new Error(err.message))
			}
		}
	)
}

async function listParticipants() {
    var gAuth2 = gapi.auth2.getAuthInstance();
    if (gAuth2.isSignedIn.get()){
        var user = gAuth2.currentUser.get();

        return jQuery.ajax(
            {
                url: '/portal/portal-resolve-participants',
                data: {
                    email: user.getBasicProfile().getEmail(),
                    id_token: user.getAuthResponse(true).id_token
                }
            }
        );
    }
    return [];
}

function generateMultyPartFileBody(
    fileName,
    fileData,
    fileType,
    boundary
){
    const boundary_str = boundary || '314159265358979323846',
        delimiter = `\r\n--${boundary_str}\r\n`,
        closeDelim = `\r\n--${boundary_str}--`;

    let contentType = fileType || 'application/octet-stream',
        metadata = JSON.stringify({
			'name': fileName,
			'mimeType': contentType
        }),
        body = btoa(fileData);

        multypartBody = (
            `${delimiter}Content-Type: application/json\r\n\r\n${metadata}` +
            `${delimiter}Content-Type: ${contentType}\r\n` + 
            `Content-Transfer-Encoding: base64\r\n\r\n${body}${closeDelim}`
        );

    return {
        'body': multypartBody,
        'contentType': contentType,
        'boundary': boundary_str
    }
}

function getBucketName(project, participant_number){
    return `${project}_participant_${participant_number}`;
}

function isADmin(role){
    return _.isEqual(_.trim(_.toLower(role)), 'admin');
}