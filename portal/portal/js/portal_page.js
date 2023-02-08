"use strict";
const PROJECTS = ['production-epbp'],
    SIGN_IN_CONTAINER_SEL = '.navbar-nav.navbar-jbb-login',
    SIGN_IN_BTN_SEL = '.navbar-nav.navbar-jbb-login .nav-item .nav-link.jbb-login-btn',

    SIGN_OUT_CONTAINER_SEL = '.navbar-nav.navbar-jbb-logout',
    USER_NAME_EL_SEL = '.dropdown .dropdown-toggle > .user-name',
    USER_NAME_DETAIL_EL_SEL = '.dropdown-menu .dropdown-item.user-name',	
    SIGN_OUT_ITEM = `.navbar-nav.navbar-jbb-logout .dropdown-menu .dropdown-item.logout`,	
    PAGE_LOADER_CNTNR_SEL = '.page-loader-container',
    PORTAL_CNTNR_SEL = 'div.jbb-portal-container',
    EMPTY_PARTICIPANT_WARNING = `
        You have no assigned participants. 
        Please contact administrator to assign
    `;

    // EMPTY_BACKGROUND = `
    //     <div class="bg-image d-flex justify-content-center align-items-center "></div> 			
    // `,
    // BACKGROUND_CONTAINER_SEL = ".bg-container",
    // BACKROUND_IMG_CONTAINER_SEL = ".bg-container .bg-img-container",
    // BODY_CONTAINER_SEL = ".body.container-fluid",
    // PRELOADER_SEL = "#preloader",

    // EMPTY_PARTICIPANT_WARNING = `
    //     <div class="container-fluid d-flex flex-column p-0 bg-body">
    //     <div class="row flex-grow-1 ms-0 me-0 p-1">
    //         <div class="col-12 d-flex flex-column justify-content-center border p-0">
    //         <div class="card border bg-body ms-auto me-auto w-25 opacity-75">
    //             <div class="card-header bg-light p-1">
    //             <h3 class="text-danger text-center m-1">
    //                 <i class="bi-exclamation-octagon-fill"></i> 
    //                 WARNING
    //             </h3>
    //             </div>
    //             <div class="card-body bg-warning text-center">
    //             <h5>You have no assigned participants.</h5>
    //             </div>
    //             <div class="card-footer bg-light text-center text-danger p-1">
    //             <h3 class="m-1">Please contact your administrator to assign.</h3>
    //             </div>
    //         </div>
    //         </div>
    //     </div>
    //     </div>
    // `;
// $(document)
//     .on('click', '.fa-chevron-up', function () {
//         $(this).parents('.card-body')
//             .find('.fa-chevron-up').hide().end()
//             .find('.fa-chevron-down').show().end()
//             .find('.toggle-data').toggle();

//     })
//     .on('click', '.fa-chevron-down', function () {
//         $(this).parents('.card-body')
//             .find('.fa-chevron-up').show().end()
//             .find('.fa-chevron-down').hide().end()
//             .find('.toggle-data').toggle();
//     })

$(document).ready(function() {
    Handlebars.registerHelper(
        'cutFilename', 
        function(value) {
            return (value || '').split('/').at(-1);
        });
});

// ===========================================

function initClient() {
    GAPI_INITED = true;
    tryToInitApplication();
}

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
    renderPortal();
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
            removePageLoader($(PAGE_LOADER_CNTNR_SEL));
            // removeDashboard();
        }
    );
}

function _initialisParticipants(model, projects, participants){

    _.forEach(projects, (project) => {
        model.participants = _.concat(
            model.participants,
            _.map(participants, (participant) => {
                var bucket_name = getBucketName(project, participant.number);
                return {
                    number: participant.number,
                    env: project,
                    bucket: bucket_name,
                    idx: bucket_name,
                    admin: isADmin(participant.role),
                    status: 'success',
                    validation: [],
                    fileName: 'config/participant.xml',
                    config: [],
                    meters: [],
                    pushes: [],
                    properties: [],
                    codes: []                    
                }
            })
        )

    });
} 

function renderPortalBody(data){
    var template = Handlebars.templates.admin_portal;
    removePortalBody();
    hidePortalBody();
    $(PORTAL_CNTNR_SEL).append(template(data))
}

function showPortalBody(){
    $(PORTAL_CNTNR_SEL).removeClass('visually-hidden');
}

function hidePortalBody(){
    $(PORTAL_CNTNR_SEL).addClass('visually-hidden');
}

function removePortalBody(){
    $(PORTAL_CNTNR_SEL).empty()
}

function renderHighCharts(model){
    _.forEach(model.participants, (participant) => {
        _.forEach(participant.properties, (property) => {
            Highcharts.chart(
                `chart_${property.idx}`, 
                {
                    chart: {type: 'line'},
                    legend: {
                        enabled: true,
                        layout: 'vertical',
                        align: 'left',
                        verticalAlign: 'middle',
                    },
                    title: {
                        text: '',
                        margin: 5,
                        style: {
                            color: "#333333",
                            fontSize: "12px"
                        }
                    },

                    yAxis: {
                        labels: {enabled: false},
                        title: {text: null}
                    },

                    xAxis: {
                        type: 'datetime',
                        labels: {enabled: true},
                        title: {text: null}
                    },

                    series: _.map(chartTypes, value => {
                        return {
                            name: value,
                            data: property[`data_${_.toLower(value)}`]													
                        }
                    })
                }
            )
        });
    });

}

async function renderPortal(){
    var auth2 = gapi.auth2.getAuthInstance(),
        user,
        model;

    if (auth2.isSignedIn.get()){
        user = auth2.currentUser.get();
        model = {
            name: user.getBasicProfile().getName(),
            participants: []
        };
        console.log("Loading user assigned participants;");
        listParticipants().then(
            async (participants) => {
                var usr_participants = _.orderBy(
                    JSON.parse(participants), 
                    ["number"]
                );
                console.log("Loading configuration of all assigned participants");
                if (_.isEmpty(usr_participants)){
                    removePageLoader();
                    renderWarning(
                        'Absent Participants', 
                        EMPTY_PARTICIPANT_WARNING
                    );
                } else {
                    console.log("Starting Participant models initialization");
                    _initialisParticipants(model, PROJECTS, usr_participants);
                    console.log("Initialized Participant models");

                    console.log("Strting User information load");                    
                    const promises = await model.participants.map(async (as_model) => {
                        await modelParticipant(as_model);
                    });
                    await Promise.all(promises);                    
                    console.log("User information loaded");

                    // model.participants[0].validation = [
                    //     'No meter weights defined in BigQuery DW',
                    //     'Error. XML needs validation against XML Schema.', 
                    //     `Error in push/pull config in participant.xml: sdtfhbgrtsdf`
                    // ];
                    // model.participants[0].properties[0].weights = [];

                    removePageLoader();
                    renderPortalBody(model);
                    showPortalBody();
                    renderHighCharts(model);
                    initDropDown();
                }
            }
        );
    } else {
        console.error("Sign in before call render portal");
        initializeHeaderButtons();
        removePageLoader();
        renderWarning(
            'Wrong Order', 
            "Sign in before call render portal"
        );  
    }








    // if (auth2.isSignedIn.get()){
    //     var user = auth2.currentUser.get();
        // model = {
        //     name: user.getBasicProfile().getName(),
        //     participants: []
        // };
    //     showSpinner();

    //     listParticipants().then(
    //         async function (participants){
    //             var usr_participants = _.orderBy(
    //                 JSON.parse(participants), 
    //                 ["number"]
    //             );
    //             if (usr_participants.length == 0){
    //                 enableSignOutBtn();
    //                 hideBackground();
    //                 hideSpinner();
    //                 $('#body').empty();
    //                 $('#body').html(EMPTY_PARTICIPANT_WARNING);

    //             } else {
    //                 _.forEach(
    //                     usr_participants, 
    //                     function(participant){
    //                         _.forEach(
    //                             BUCKET_NAME_PREFIX, 
    //                             function(bucket){
    //                                 var bucket_name = `${bucket}${participant.number}`;
    //                                 model.participants.push(
    //                                     {
    //                                         number: participant.number,
    //                                         env: bucket.split("_participant")[0].split("_").slice(-1).pop(),
    //                                         bucket: bucket_name,
    //                                         idx: bucket_name,
    //                                         admin: participant.role == 'admin',
    //                                         status: 'success',
    //                                         validation: [],
    //                                         fileName: 'config/participant.xml',
    //                                         config: [],
    //                                         meters: [],
    //                                         pushes: [],
    //                                         properties: [],
    //                                         codes: []
    //                                     }
    //                                 )
    //                             }
    //                         )
    //                     }
    //                 );
    //                 console.log('************');

    //                 // model = {
    //                 // 	name: user.getBasicProfile().getName(),
    //                 // 	participants: _.slice(model.participants, 0, 2)
    //                 // };

    //                 const promises = await model.participants.map(async (as_model) => {
    //                     await modelParticipant(as_model);
    //                 });
    //                 await Promise.all(promises);

    //                 console.log('************');
    //                 model.now = new Date().toISOString().slice(0, 19);						
    //                 console.log("All collected data for the given user", model);

    //                 $("#body").empty();

    //                 $.get(TEMPLATE_PATH, data => {
    //                     enableSignOutBtn();
    //                     hideBackground();
    //                     hideSpinner();
    //                     $('#body').append(
    //                         Handlebars.compile(data)(model)
    //                     );

    //                     const tooltipTriggerList = document.querySelectorAll('[data-bs-toggle="tooltip"]');
    //                     [...tooltipTriggerList].map(tooltipTriggerEl => new bootstrap.Tooltip(tooltipTriggerEl));

    //                     for (const partIdx in model.participants) {
    //                         for (const propIdx in model.participants[partIdx].properties) {
    //                             const property = model.participants[partIdx].properties[propIdx]

    //                             Highcharts.chart('chart_' + property.idx, {
    //                                 chart: {
    //                                     type: 'line'
    //                                 },

    //                                 legend: {
    //                                     enabled: true,
    //                                     layout: 'vertical',
    //                                     align: 'left',
    //                                     verticalAlign: 'middle',
    //                                                                             },

    //                                 title: {
    //                                     text: 'Property Data in the Data Warehouse, last 72 hours',
    //                                     margin: 5,
    //                                     style: {
    //                                         color: "#333333",
    //                                         fontSize: "12px"
    //                                     }
    //                                 },

    //                                 yAxis: {
    //                                     labels: {
    //                                         enabled: false
    //                                     },
    //                                     title: {
    //                                         text: null
    //                                     }
    //                                 },

    //                                 xAxis: {
    //                                     type: 'datetime',
    //                                     labels: {
    //                                         enabled: true
    //                                     },
    //                                     title: {
    //                                         text: null
    //                                     }
    //                                 },

    //                                 series: _.map(chartTypes, value => {
    //                                     return {
    //                                         name: value,
    //                                         data: property[`data_${_.toLower(value)}`]													
    //                                     }
    //                                 })
    //                             })

    //                         }
    //                     }

    //                 });
    //             }
    //         }
    //     );
        
    // } else {
    //     console.error("Sign in before call render portal");
    //     // TODO: Add toster with message here
    // }
}