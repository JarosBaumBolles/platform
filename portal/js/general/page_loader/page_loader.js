var PageLoaderRunner = new TimelineMax({repeat:-1,repeatDelay:.01,yoyo:true});


const PAGE_LOADER_CONTAINER_SEL = 'div.jbb-page-loader',
      FIRST_SPIN_CONTENT = `${PAGE_LOADER_CONTAINER_SEL} .loader-start > span`,
      SECOND_SPIN_CONTENT = `${PAGE_LOADER_CONTAINER_SEL} .loader-end > span`,
      PAGE_LOADER_HTML = `        
        <div class="d-flex flex-grow-1 align-items-center justify-content-center flex-row jbb-page-loader">
            <div class="d-flex align-items-center justify-content-end">
                <div class="loader-start">
                    <span>B</span>
                    <span>E</span>
                    <span>N</span>
                    <span>C</span>
                    <span>H</span>
                    <span>M</span>
                    <span>A</span>
                    <span>R</span>
                    <span>K</span>
                </div>
            </div>
            <div class="d-flex align-items-center justify-content-start">
                <div class="loader-end">
                    <span>8</span>
                    <span>7</span>
                    <span>6</span>
                    <span>0</span>
                </div>
            </div>
        </div>    
    `;

function renderPageLoader(){
    $(PAGE_LOADER_CONTAINER_SEL).empty().html(PAGE_LOADER_HTML);
    showPageLoader();
    startPageLoader();
}

function showPageLoader(){
    $(PAGE_LOADER_CONTAINER_SEL).removeClass('visually-hidden');
}

function hidePageLoader(){
    $(PAGE_LOADER_CONTAINER_SEL).addClass('visually-hidden');
}

function initPageLoader(){
    PageLoaderRunner.staggerTo(
        FIRST_SPIN_CONTENT, 
        .5, 
        {
            color: "#BB972E",
            scale:1.05
        }, 
        0.2
    );

    PageLoaderRunner.staggerTo(
        SECOND_SPIN_CONTENT, 
        .5, 
        {
            color: "#1A4589",
            scale:1.05
        }, 
        0.2
    );
}

function stopPageLoader(){
    PageLoaderRunner.clear();
    $(FIRST_SPIN_CONTENT).removeAttr('style');
    $(SECOND_SPIN_CONTENT).removeAttr('style');
}

function startPageLoader() {
    stopPageLoader();
    initPageLoader();
}

function pausePageLoader(){
    PageLoaderRunner.pause();
}

function resumePageLoader(){
    PageLoaderRunner.resume();
}

function removePageLoader(){
    stopPageLoader();
    $(PAGE_LOADER_CONTAINER_SEL).empty();
    hidePageLoader();
}