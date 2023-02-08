"use strict";
/*
	Private Dasboard code
*/

const SIGN_IN_CONTAINER_SEL = '.navbar-nav.navbar-jbb-login',
    SIGN_IN_BTN_SEL = '.navbar-nav.navbar-jbb-login .nav-item .nav-link.jbb-login-btn',

    SIGN_OUT_CONTAINER_SEL = '.navbar-nav.navbar-jbb-logout',
    USER_NAME_EL_SEL = '.dropdown .dropdown-toggle > .user-name',
    USER_NAME_DETAIL_EL_SEL = '.dropdown-menu .dropdown-item.user-name',	
    SIGN_OUT_ITEM = `.navbar-nav.navbar-jbb-logout .dropdown-menu .dropdown-item.logout`,	
    PROJECTS = ['production-epbp'],
    SERVER_TZ = "UTC",
    GENERAL_DATE_FORMAT="YYYY-MM-DDTHH:mm:ss",
    DEFAULT_DASHBOARD_URI = 'datastudio.google.com',
	DASHBOARD_URLS = {
        "production-epbp": (
            'https://datastudio.google.com/embed/reporting/' + 
            'c1a3bf5e-ae01-4ca5-b05f-0127fbcc82b6/page/p_nsipm3ylzc'
	    )
    },
    EMPTY_PARTICIPANT_WARNING = `
        You have no assigned participants. 
        Please contact administrator to assign
    `,               
    EMPTY_PROPERTIES_WARNING = "Looks like You have no configured or assigned properties.";            

let localTz = moment.tz.guess(),
	userPropertiesQuery = (participants) => `
        SELECT property_uri
        FROM \`standardized_new.properties\`
            WHERE ref_participant_id in (${participants})   
    `;

function getDashboardUri(environment){
    var uri = _.get(DASHBOARD_URLS, environment);
    return _.isEmpty(uri) ? DEFAULT_DASHBOARD_URI : uri;
}

async function getUserProperties(participants, dataset) {
    var propertiesQuery = userPropertiesQuery(participants.join(',')),
        results = [];

    console.log('---------------------------------------------');
    console.log(`Prperties query is ${propertiesQuery}`);
    console.log('---------------------------------------------');

    const queryData = await query(dataset, propertiesQuery);

    _.map(queryData, row => results.push(row.property_uri));

    return results;
}

function initClient() {
    GAPI_INITED = true;
    tryToInitApplication();
};

function tryToInitApplication(){
    if (GAPI_INITED) {
        console.info(
            "[INIT APPLICATION] Started Application"
        );
        initializeHeaderButtons();
    }
};

function initializeHeaderButtons (){
    showSignInMenu();
    hideSignOutDropDownMenu();
    $(SIGN_IN_BTN_SEL).click(triggerGoogleSignInDialog);
    $(SIGN_OUT_ITEM).click(SignOutHandler);
}

function showSignInMenu(){
    $(SIGN_IN_CONTAINER_SEL).removeClass("visually-hidden");
}

function hideSignInMenu(){
    $(SIGN_IN_CONTAINER_SEL).addClass("visually-hidden");
}		

function showSignOutDropDownMenu(userProfile){

    var menuCntrEl = $(SIGN_OUT_CONTAINER_SEL),
        usrNameEl = menuCntrEl.find(USER_NAME_EL_SEL),
        useDetailInfoEl = menuCntrEl.find(USER_NAME_DETAIL_EL_SEL),
        usrName = userProfile.getName(),
        userIconURI = userProfile.getImageUrl();

    if (userIconURI) {
        var avatarEl = useDetailInfoEl.find('img');
        avatarEl.removeClass('visually-hidden');
        avatarEl.attr("src", userIconURI);
    } else {
        useDetailInfoEl.find('i').removeClass('visually-hidden');
    }

    usrNameEl.text(usrName);
    useDetailInfoEl.find('span').text(usrName);
    menuCntrEl.removeClass("visually-hidden");
}

function hideSignOutDropDownMenu(){				
    var menuCntrEl = $(SIGN_OUT_CONTAINER_SEL),
        usrNameEl = menuCntrEl.find(USER_NAME_EL_SEL),
        useDetailInfoEl = menuCntrEl.find(USER_NAME_DETAIL_EL_SEL),
        avatarEl = useDetailInfoEl.find('img');
    
    useDetailInfoEl.find('i').addClass('visually-hidden');
    avatarEl.addClass('visually-hidden');
    avatarEl.attr("src", '');
    useDetailInfoEl.find('span').text('');

    menuCntrEl.addClass("visually-hidden");
}

function googleSignInHandler(googleUser){
    console.log('--------------------------------------------------');
    hideSignInMenu();
    showSignOutDropDownMenu(googleUser.getBasicProfile());

    renderPageLoader();

    renderPrivateDashboards();
}

function triggerGoogleSignInDialog(event){
    event.preventDefault();
    $('#google-signin div.abcRioButtonContentWrapper').trigger("click");
}

function SignOutHandler(event){
    console.info("Sign Out Click");
    var auth2 = gapi.auth2.getAuthInstance();
    auth2.signOut().then(
        function () {
            console.log('User signed out.');
            hideSignOutDropDownMenu();
            showSignInMenu();
            removePageLoader();
            removeDashboard();
        }
    );
}    
            
async function renderPrivateDashboards(){
    var auth2 = gapi.auth2.getAuthInstance();
    if (auth2.isSignedIn.get()){
        listParticipants().then(
            async function (participants){
                var usr_participants = _.orderBy(
                    JSON.parse(participants), 
                    ["number"]
                );
                if (usr_participants.length == 0){
                    renderWarning('Absent Participants', EMPTY_PARTICIPANT_WARNING);
                } else {
                    console.info("sdzrbgvas");
                    var usrParticipantsIds = _.map(usr_participants, 'number'),
                        dashboards_cfg = [];

                    const promises = await PROJECTS.map(async (prj) => {
                        const assignedProperties = await getUserProperties(
                            usrParticipantsIds,
                            prj
                        );

                        dashboards_cfg.push({
                            'properties': assignedProperties,
                            'uri': getDashboardUri(prj)
                        });                                 
                    });
                    await Promise.all(promises);

                    console.info("+++++++++++++++++++++++++++++++++++");

                    if (_.isEmpty(dashboards_cfg)){
                        renderWarning(
                            'Absent Properties', 
                            EMPTY_PROPERTIES_WARNING
                        );
                    } else {
                        removePageLoader();                        
                        renderDashboards(dashboards_cfg);
                        console.info('*************');
                    }                              
                }     
            }
        )           
    }
    else{
        console.error("Sign in before call render portal");
        initializeHeaderButtons();
        renderWarning(
            'Wrong Order', 
            "Sign in before call render portal"
        );        
    }               
}                     