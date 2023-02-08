const WARNING_CONTAINER_SEL = 'div.warning-container',
    WARNING_CARD_TEMPL = (title, message) => `
        <div class="card border border-warning shadow-0 mb-3">
            <div class="card-header bg-warning fs-pt-20 fw-bold">Warning</div>
            <div class="card-body">
                <h5 class="card-title fs-pt-16 fw-bold">${title}</h5>
                <p class="card-text">
                    ${message}
                </p>
            </div>
        </div>	    
    `;

function hideWarning(){
    $(WARNING_CONTAINER_SEL).removeClass('visually-hidden');
}

function showWarning(){
    $(WARNING_CONTAINER_SEL).removeClass('visually-hidden');
}

function renderWarning(title, message){
    $(WARNING_CONTAINER_SEL).empty().append(
        WARNING_CARD_TEMPL(title, message)
    );
    showWarning();    
}