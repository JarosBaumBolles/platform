const DROPDOWN_SEL = '.dropdown-menu .dropdown-submenu > .dropdown-submenu-toggle.show',
    DISABLED_CLS = 'disabled',
    SHOW_CLS = 'show';


function dropdownToggler(event){
    var toggler = $(event.currentTarget);
    console.log('---------------------------------------------------------');
    event.stopPropagation();
    event.preventDefault();
    if (!toggler.hasClass(DISABLED_CLS)) {
        toggler.toggleClass(SHOW_CLS);
    }

}

function clearMenus(event){
    _.forEach($(DROPDOWN_SEL), (toggler) => {
        console.info('*******************************************************');
        $(toggler).removeClass(SHOW_CLS);
    });
    event.stopPropagation();
    event.preventDefault();    
}

function initDropDown(){
    console.log('(((((((((((((((((((((((((((())))))))))))))))))))))))))))');
    $(DROPDOWN_SEL).click(dropdownToggler);    
    $(document).click(clearMenus);
}


