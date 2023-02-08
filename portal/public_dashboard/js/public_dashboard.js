"use strict";
/*
	Public Dasboard Code
*/

const DEFAULT_DASHBOARD_URI = 'datastudio.google.com',
    PROJECTS = ['production-epbp'], 
	DASHBOARD_URLS = {
        "production-epbp": (
            'https://datastudio.google.com/embed/reporting/24673c3f-5a83-4f7b-a6d3-15f7e6401c86/page/p_nsipm3ylzc'
	    )
    };

function getDashboardUri(environment){
    var uri = _.get(DASHBOARD_URLS, environment);
    return _.isEmpty(uri) ? DEFAULT_DASHBOARD_URI : uri;
}

function renderPublicDashboard() {
    var cfgs= _.map(PROJECTS, (prj) => {
        return {
            'properties': [],
            'uri': getDashboardUri(prj)
        }
    });

    renderDashboards(cfgs);
}

renderPublicDashboard();
