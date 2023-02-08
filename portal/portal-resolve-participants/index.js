const {
  GoogleAuth
} = require('google-auth-library');
const fetch = require('node-fetch'),
  stCodes = require('http-status-codes'),
  _ = require('lodash');

exports.resolveParticipants = async (req, res) => {
  const getParticipantsListURL = "https://admin.googleapis.com/admin/directory/v1/groups?userKey=";
  let domain = 'hourlybuildingdata.com',
      request = {
          method: 'GET',
          headers: {
              'Content-Type': 'application/json'
          }
      },
      url = new URL('https://oauth2.googleapis.com/tokeninfo');

  res.set('Access-Control-Allow-Origin', '*');

  if ( _.isEqual(req.method, 'OPTIONS')) {
      res.set('Access-Control-Allow-Methods', 'GET');
      res.set('Access-Control-Allow-Headers', 'Content-Type,Authorization');
      res.set('Access-Control-Max-Age', '3600');
      res.status(stCodes.StatusCodes.NO_CONTENT).send('');
      return;
  } else {
      if (!_.isEqual(req.method, 'GET')){
          res.status(stCodes.StatusCodes.METHOD_NOT_ALLOWED).send(
              'Only GET method is allowed'
          );
      } else {
          if (_.some([req.query.email, req.query.id_token], _.isUndefined)){
              console.error(
                  'Validation Error. Undefined email or id_token required parameter.'
              );
              res.status(stCodes.StatusCodes.BAD_REQUEST).send(
                  'Validation Error. Undefined email or id_token required parameter.'
              );
          } else {
              url.search = new URLSearchParams(
                {
                  id_token: req.query.id_token
                }
              ).toString();

              console.debug('----------------------');
              console.log("Get profile info by the given email and id_token");
              console.debug('URL', url.toString());
              console.debug('Request email', req.query.email);
              console.debug('Request id_token', req.query.id_token);

              const profileRaw = await fetch(url, request);

              console.debug('Request OK status is ', profileRaw.ok);
              console.debug('Request status is ', profileRaw.status);

              if (!profileRaw.ok || profileRaw.status != stCodes.StatusCodes.OK){
                  let errBlob = await profileRaw.blob(),
                      errMsg = await errBlob.text();        
                  console.error(
                      `Status - ${profileRaw.status}. ` + 
                      `Recieved error "${errMsg}" during ID token verification for ` + 
                      `user ${req.query.email} with token ${req.query.id_token}`
                  );
                  res.status(stCodes.StatusCodes.OK).send(JSON.stringify([]));      
              } else {
                      const profile = await profileRaw.json();

                  console.debug('----------------------');
                  console.log("Verification id token information");
                  console.debug("Profile email", profile.email);
                  console.debug("Profile Domain", profile.hd);
                  //if (profile.email == req.query.email && profile.hd == domain) {

                  if (!_.isEqual(profile.email, req.query.email)){
                      console.log(
                          `User ${req.query.email} with token ${req.query.id_token} ` + 
                          `is not allowed to request participats list.`
                      );
                      res.status(stCodes.StatusCodes.OK).send(JSON.stringify([]));
                  } else {
                      console.debug('----------------------');
                      console.log("Retrieving assigned participans;");
                      console.debug("Request internal access token");
                      const auth = new GoogleAuth(
                          {
                              scopes: `https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/admin.directory.group.readonly`
                          }
                      );
                      console.debug("GoogleAuth is created");
                      const accessToken = await auth.getAccessToken();
                      let participantUrl = `${getParticipantsListURL}${req.query.email}&domain=${domain}`;

                      console.debug("Internal auth token", accessToken);
                      console.debug("Request participants URL", participantUrl.toString());
                      request.headers.Authorization = `Bearer ${accessToken}`;

                      const responseRaw = await fetch(participantUrl, request);
                      console.debug("Request participant OK status", responseRaw.ok);
                      console.debug("Request participant status", responseRaw.status);

                      if (!responseRaw.ok || responseRaw.status != stCodes.StatusCodes.OK){
                          console.error(
                              `Recieved error during request participats list for ` + 
                              `user ${req.query.email} with token ${req.query.id_token}`
                          );
                          res.status(stCodes.StatusCodes.OK).send(JSON.stringify([]));
                      } else{
                          const response = await responseRaw.json();

                          let pattern = /participant(\d+)_(admin|operator)/;
                          if (_.isUndefined(response.groups) || !_.every(response.groups, _.isObject)){
                              console.warn(`User ${req.query.email} (token ${req.query.id_token}) ` +
                                  `with unassigned groups. Return empty list.`
                              );
                              res.status(stCodes.StatusCodes.OK).send(JSON.stringify([]));
                          }

                          let groups = _.uniqWith(
                              _.map(
                                  _.filter(_.map(response.groups, "name"), name => pattern.test(name)), 
                                  name => { 
                                      return {
                                          number: parseInt(pattern.exec(name)[1]),
                                          role: pattern.exec(name)[2]
                                      }
                                  }
                              ), 
                              _.isEqual
                          );
                          console.log(JSON.stringify(groups));
                          res.status(stCodes.StatusCodes.OK).send(JSON.stringify(groups));        
                      }        
                  }
              }        
          }
      }        
  }
};