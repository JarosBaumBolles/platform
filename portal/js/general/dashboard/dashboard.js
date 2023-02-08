const DASHBOARD_CONTAINER_SEL = 'div.dashboard-container';

let DASHBOARD_TEMPLATE = (uri) => `
    <iframe 
        class="dashboard-iframe"
        src="${uri}" 
        frameborder="0" 
        allowfullscreen
    ></iframe>
`;


function renderDashboardHTML(dashboardUri, properties){

    if (_.isEmpty(properties)){
        return DASHBOARD_TEMPLATE(dashboardUri);
    }

    let params = encodeURIComponent(
        JSON.stringify(
            {
                  "ds0.property_uri": `${properties.join(',')}`,
                  "ds181.property_uri": `${properties.join(',')}`,
                  "ds182.property_uri": `${properties.join(',')}`,
            }
        )
    );
    return DASHBOARD_TEMPLATE(`${dashboardUri}?params=${params}`);
}


function renderDashboards(dashboardsProperties){
    let dashboardsHtml = _.map(dashboardsProperties, dashboard=>{
        return renderDashboardHTML(
            dashboard.uri,
            dashboard.properties
        )
    });

    $(DASHBOARD_CONTAINER_SEL).empty().append(dashboardsHtml.join());
    showDashboard();
}


function showDashboard() {
    $(DASHBOARD_CONTAINER_SEL).removeClass('visually-hidden');
}

function hideDashboard(){
    $(DASHBOARD_CONTAINER_SEL).addClass('visually-hidden');
}

function removeDashboard() {
    $(DASHBOARD_CONTAINER_SEL).empty();
}