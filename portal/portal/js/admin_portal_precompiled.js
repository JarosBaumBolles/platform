(function() {
  var template = Handlebars.template, templates = Handlebars.templates = Handlebars.templates || {};
templates['admin_portal'] = template({"1":function(container,depth0,helpers,partials,data) {
    var helper, alias1=depth0 != null ? depth0 : (container.nullContext || {}), alias2=container.hooks.helperMissing, alias3="function", alias4=container.escapeExpression, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "    <a id=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"prefix") || (depth0 != null ? lookupProperty(depth0,"prefix") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"prefix","hash":{},"data":data,"loc":{"start":{"line":2,"column":11},"end":{"line":2,"column":21}}}) : helper)))
    + "_button_"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":2,"column":29},"end":{"line":2,"column":36}}}) : helper)))
    + "\"\n       class=\"link-button\"\n       data-bucket=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"bucket") || (depth0 != null ? lookupProperty(depth0,"bucket") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"bucket","hash":{},"data":data,"loc":{"start":{"line":4,"column":20},"end":{"line":4,"column":30}}}) : helper)))
    + "\"\n       data-path=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"fileName") || (depth0 != null ? lookupProperty(depth0,"fileName") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"fileName","hash":{},"data":data,"loc":{"start":{"line":5,"column":18},"end":{"line":5,"column":30}}}) : helper)))
    + "\"\n       data-link=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"prefix") || (depth0 != null ? lookupProperty(depth0,"prefix") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"prefix","hash":{},"data":data,"loc":{"start":{"line":6,"column":18},"end":{"line":6,"column":28}}}) : helper)))
    + "_link_"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":6,"column":34},"end":{"line":6,"column":41}}}) : helper)))
    + "\"\n       onclick=\"download_from_bucket(event)\">\n        "
    + alias4(((helper = (helper = lookupProperty(helpers,"text") || (depth0 != null ? lookupProperty(depth0,"text") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"text","hash":{},"data":data,"loc":{"start":{"line":8,"column":8},"end":{"line":8,"column":16}}}) : helper)))
    + "\n    </a>\n\n    <a download=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"filename") || (depth0 != null ? lookupProperty(depth0,"filename") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"filename","hash":{},"data":data,"loc":{"start":{"line":11,"column":17},"end":{"line":11,"column":29}}}) : helper)))
    + "\"\n       id=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"prefix") || (depth0 != null ? lookupProperty(depth0,"prefix") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"prefix","hash":{},"data":data,"loc":{"start":{"line":12,"column":11},"end":{"line":12,"column":21}}}) : helper)))
    + "_link_"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":12,"column":27},"end":{"line":12,"column":34}}}) : helper)))
    + "\"\n       style=\"display: none;\">\n        Save XML\n    </a>\n";
},"3":function(container,depth0,helpers,partials,data) {
    var helper, alias1=depth0 != null ? depth0 : (container.nullContext || {}), alias2=container.hooks.helperMissing, alias3="function", alias4=container.escapeExpression, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "    <label class=\"link-button\" for=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"prefix") || (depth0 != null ? lookupProperty(depth0,"prefix") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"prefix","hash":{},"data":data,"loc":{"start":{"line":19,"column":36},"end":{"line":19,"column":46}}}) : helper)))
    + "_upload_label_"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":19,"column":60},"end":{"line":19,"column":67}}}) : helper)))
    + "\">"
    + alias4(((helper = (helper = lookupProperty(helpers,"text") || (depth0 != null ? lookupProperty(depth0,"text") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"text","hash":{},"data":data,"loc":{"start":{"line":19,"column":69},"end":{"line":19,"column":77}}}) : helper)))
    + "</label>\n    <input type=\"file\"\n           id=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"prefix") || (depth0 != null ? lookupProperty(depth0,"prefix") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"prefix","hash":{},"data":data,"loc":{"start":{"line":21,"column":15},"end":{"line":21,"column":25}}}) : helper)))
    + "_upload_label_"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":21,"column":39},"end":{"line":21,"column":46}}}) : helper)))
    + "\"\n           style=\"display: none;\"\n           data-status=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"prefix") || (depth0 != null ? lookupProperty(depth0,"prefix") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"prefix","hash":{},"data":data,"loc":{"start":{"line":23,"column":24},"end":{"line":23,"column":34}}}) : helper)))
    + "_upload_status_"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":23,"column":49},"end":{"line":23,"column":56}}}) : helper)))
    + "\"\n           data-bucket=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"bucket") || (depth0 != null ? lookupProperty(depth0,"bucket") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"bucket","hash":{},"data":data,"loc":{"start":{"line":24,"column":24},"end":{"line":24,"column":34}}}) : helper)))
    + "\"\n           data-path=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"fileName") || (depth0 != null ? lookupProperty(depth0,"fileName") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"fileName","hash":{},"data":data,"loc":{"start":{"line":25,"column":22},"end":{"line":25,"column":34}}}) : helper)))
    + "\"\n           onchange='uploadToStorageBucket(event)'\n           multiple />\n    <span id=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"prefix") || (depth0 != null ? lookupProperty(depth0,"prefix") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"prefix","hash":{},"data":data,"loc":{"start":{"line":28,"column":14},"end":{"line":28,"column":24}}}) : helper)))
    + "_upload_status_"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":28,"column":39},"end":{"line":28,"column":46}}}) : helper)))
    + "\" class=\"message\"></span>\n";
},"5":function(container,depth0,helpers,partials,data) {
    var helper, alias1=depth0 != null ? depth0 : (container.nullContext || {}), alias2=container.hooks.helperMissing, alias3="function", alias4=container.escapeExpression, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "    <a id=\"property_dashboard_button_"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":33,"column":37},"end":{"line":33,"column":44}}}) : helper)))
    + "\"\n        class=\"link-button\"\n        href="
    + alias4(((helper = (helper = lookupProperty(helpers,"dataStudioLink") || (depth0 != null ? lookupProperty(depth0,"dataStudioLink") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"dataStudioLink","hash":{},"data":data,"loc":{"start":{"line":35,"column":13},"end":{"line":35,"column":31}}}) : helper)))
    + "\n        target=\"_blank\"\n    >\n        Property Dashboard\n    </a>\n\n";
},"7":function(container,depth0,helpers,partials,data) {
    return "    <div class=\"ps-1 pe-1 h-100\">\n        <div class=\"vr h-100\"></div>\n    </div>\n";
},"9":function(container,depth0,helpers,partials,data) {
    var helper, alias1=depth0 != null ? depth0 : (container.nullContext || {}), alias2=container.hooks.helperMissing, alias3="function", alias4=container.escapeExpression, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "    <a\n        id=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"prefix") || (depth0 != null ? lookupProperty(depth0,"prefix") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"prefix","hash":{},"data":data,"loc":{"start":{"line":71,"column":12},"end":{"line":71,"column":22}}}) : helper)))
    + "_button_"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":71,"column":30},"end":{"line":71,"column":37}}}) : helper)))
    + "\"\n        class=\"btn btn-tag btn-rounded btn-outline-secondary text-white\"\n\n        data-bucket=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"bucket") || (depth0 != null ? lookupProperty(depth0,"bucket") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"bucket","hash":{},"data":data,"loc":{"start":{"line":74,"column":21},"end":{"line":74,"column":31}}}) : helper)))
    + "\"\n        data-path=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"fileName") || (depth0 != null ? lookupProperty(depth0,"fileName") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"fileName","hash":{},"data":data,"loc":{"start":{"line":75,"column":19},"end":{"line":75,"column":31}}}) : helper)))
    + "\"\n        data-link=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"prefix") || (depth0 != null ? lookupProperty(depth0,"prefix") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"prefix","hash":{},"data":data,"loc":{"start":{"line":76,"column":19},"end":{"line":76,"column":29}}}) : helper)))
    + "_link_"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":76,"column":35},"end":{"line":76,"column":42}}}) : helper)))
    + "\"\n        onclick=\"download_from_bucket(event)\"\n\n        data-mdb-toggle=\"tooltip\" \n        data-mdb-placement=\"right\"\n        title=\"Googlel bucket assigned to the Participant\"\n    >\n        <i class=\"fa-sharp fa-solid fa-download\"></i>\n        "
    + alias4(((helper = (helper = lookupProperty(helpers,"text") || (depth0 != null ? lookupProperty(depth0,"text") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"text","hash":{},"data":data,"loc":{"start":{"line":84,"column":8},"end":{"line":84,"column":16}}}) : helper)))
    + "\n    </a>\n    <a download=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"filename") || (depth0 != null ? lookupProperty(depth0,"filename") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"filename","hash":{},"data":data,"loc":{"start":{"line":86,"column":17},"end":{"line":86,"column":29}}}) : helper)))
    + "\"\n    id=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"prefix") || (depth0 != null ? lookupProperty(depth0,"prefix") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"prefix","hash":{},"data":data,"loc":{"start":{"line":87,"column":8},"end":{"line":87,"column":18}}}) : helper)))
    + "_link_"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":87,"column":24},"end":{"line":87,"column":31}}}) : helper)))
    + "\"\n    style=\"display: none;\">\n        Save XML\n    </a> \n";
},"11":function(container,depth0,helpers,partials,data) {
    var helper, alias1=depth0 != null ? depth0 : (container.nullContext || {}), alias2=container.hooks.helperMissing, alias3="function", alias4=container.escapeExpression, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return " <li>\n    <a \n        class=\"dropdown-node\" \n        id=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"_id") || (depth0 != null ? lookupProperty(depth0,"_id") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"_id","hash":{},"data":data,"loc":{"start":{"line":98,"column":12},"end":{"line":98,"column":19}}}) : helper)))
    + "\"\n        title=\"Configuration file is located at gs://"
    + alias4(((helper = (helper = lookupProperty(helpers,"_bucket") || (depth0 != null ? lookupProperty(depth0,"_bucket") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"_bucket","hash":{},"data":data,"loc":{"start":{"line":99,"column":53},"end":{"line":99,"column":64}}}) : helper)))
    + "/"
    + alias4(((helper = (helper = lookupProperty(helpers,"_name") || (depth0 != null ? lookupProperty(depth0,"_name") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"_name","hash":{},"data":data,"loc":{"start":{"line":99,"column":65},"end":{"line":99,"column":74}}}) : helper)))
    + "\"                \n        data-bucket=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"_bucket") || (depth0 != null ? lookupProperty(depth0,"_bucket") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"_bucket","hash":{},"data":data,"loc":{"start":{"line":100,"column":21},"end":{"line":100,"column":32}}}) : helper)))
    + "\"\n        data-path=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"_name") || (depth0 != null ? lookupProperty(depth0,"_name") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"_name","hash":{},"data":data,"loc":{"start":{"line":101,"column":19},"end":{"line":101,"column":28}}}) : helper)))
    + "\"\n        data-link=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"_link") || (depth0 != null ? lookupProperty(depth0,"_link") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"_link","hash":{},"data":data,"loc":{"start":{"line":102,"column":19},"end":{"line":102,"column":28}}}) : helper)))
    + "\"\n        onclick=\"download_from_bucket(event)\"                               \n        href=\"#\"\n    >\n        <div class=\"item-icon\">\n            <i class=\"bi bi-download\"></i>\n        </div>\n        <div class=\"item-title\">\n            "
    + alias4(((helper = (helper = lookupProperty(helpers,"_title") || (depth0 != null ? lookupProperty(depth0,"_title") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"_title","hash":{},"data":data,"loc":{"start":{"line":110,"column":12},"end":{"line":110,"column":22}}}) : helper)))
    + "\n        </div>\n        <div class=\"item-caret\"></div>                                          \n    </a>\n</li>\n";
},"13":function(container,depth0,helpers,partials,data) {
    var stack1, helper, alias1=depth0 != null ? depth0 : (container.nullContext || {}), alias2=container.hooks.helperMissing, alias3="function", alias4=container.escapeExpression, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "    <nav class=\"navbar navbar-dropdown navbar-light navbar-rounded navbar-outline\">\n        <div class=\"btn-group dropstart\">\n            <button \n                class=\"btn btn-default dropdown-toggle\" \n                type=\"button\" \n                data-mdb-toggle=\"dropdown\"\n            >\n                <div class=\"dropdown-icon\">              \n"
    + ((stack1 = lookupProperty(helpers,"unless").call(alias1,((stack1 = (depth0 != null ? lookupProperty(depth0,"validation") : depth0)) != null ? lookupProperty(stack1,"length") : stack1),{"name":"unless","hash":{},"fn":container.program(14, data, 0),"inverse":container.program(16, data, 0),"data":data,"loc":{"start":{"line":127,"column":20},"end":{"line":131,"column":31}}})) != null ? stack1 : "")
    + "                </div>\n                <div class=\"dropdown-title\" >Participant Configuration</div>\n                <div class=\"dropdown-caret\">\n                    <i class=\"fas\"></i>\n                </div>\n            </button>\n            <ul class=\"dropdown-menu\">\n                <li>\n                    <a \n                        class=\"dropdown-node\" \n                        id=\"download_config_"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":142,"column":44},"end":{"line":142,"column":51}}}) : helper)))
    + "\"\n                        title=\"Configuration file is located at gs://"
    + alias4(((helper = (helper = lookupProperty(helpers,"bucket") || (depth0 != null ? lookupProperty(depth0,"bucket") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"bucket","hash":{},"data":data,"loc":{"start":{"line":143,"column":69},"end":{"line":143,"column":79}}}) : helper)))
    + "/"
    + alias4(((helper = (helper = lookupProperty(helpers,"fileName") || (depth0 != null ? lookupProperty(depth0,"fileName") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"fileName","hash":{},"data":data,"loc":{"start":{"line":143,"column":80},"end":{"line":143,"column":92}}}) : helper)))
    + "\"                \n                        data-bucket=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"bucket") || (depth0 != null ? lookupProperty(depth0,"bucket") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"bucket","hash":{},"data":data,"loc":{"start":{"line":144,"column":37},"end":{"line":144,"column":47}}}) : helper)))
    + "\"\n                        data-path=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"fileName") || (depth0 != null ? lookupProperty(depth0,"fileName") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"fileName","hash":{},"data":data,"loc":{"start":{"line":145,"column":35},"end":{"line":145,"column":47}}}) : helper)))
    + "\"\n                        data-link=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"prefix") || (depth0 != null ? lookupProperty(depth0,"prefix") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"prefix","hash":{},"data":data,"loc":{"start":{"line":146,"column":35},"end":{"line":146,"column":45}}}) : helper)))
    + "_link_"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":146,"column":51},"end":{"line":146,"column":58}}}) : helper)))
    + "\"\n                        onclick=\"download_from_bucket(event)\"                               \n                        href=\"#\"\n                    >\n                        <div class=\"item-icon\">\n                            <i class=\"bi bi-download\"></i>\n                        </div>\n                        <div class=\"item-title\">\n                            Save XML\n                        </div>\n                        <div class=\"item-caret\"></div>                                          \n                    </a>\n                </li>\n                <li><hr class=\"dropdown-divider\" /></li>\n"
    + ((stack1 = lookupProperty(helpers,"unless").call(alias1,((stack1 = (depth0 != null ? lookupProperty(depth0,"validation") : depth0)) != null ? lookupProperty(stack1,"length") : stack1),{"name":"unless","hash":{},"fn":container.program(18, data, 0),"inverse":container.program(20, data, 0),"data":data,"loc":{"start":{"line":160,"column":16},"end":{"line":203,"column":27}}})) != null ? stack1 : "")
    + "            </ul>        \n        </div>\n    </nav>\n";
},"14":function(container,depth0,helpers,partials,data) {
    return "                        <i class=\"bi bi-check-square-fill success\"></i>\n";
},"16":function(container,depth0,helpers,partials,data) {
    return "                        <i class=\"bi bi-exclamation-square-fill danger\"></i>\n";
},"18":function(container,depth0,helpers,partials,data) {
    return "                    <li class=\"dropdown-submenu\">\n                        <a class=\"dropdown-submenu-toggle dropdown-node disabled\" href=\"#\">      \n                            <div class=\"item-icon\">\n                                <span class=\"badge badge-success\">0</span>\n                            </div>    \n                            <div class=\"item-title\">\n                                <div>{XML Errors}</div>\n                            </div>\n                            <div class=\"item-caret\">\n                                <i class=\"fas\"></i>\n                            </div>                                                                                     \n                        </a>                    \n                    </li>\n";
},"20":function(container,depth0,helpers,partials,data) {
    var stack1, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "                    <li class=\"dropdown-submenu\">\n                        <a class=\"dropdown-submenu-toggle dropdown-node\" href=\"#\">      \n                            <div class=\"item-icon\">                                                     \n                                <span class=\"badge badge-danger\">"
    + container.escapeExpression(container.lambda(((stack1 = (depth0 != null ? lookupProperty(depth0,"validation") : depth0)) != null ? lookupProperty(stack1,"length") : stack1), depth0))
    + "</span>\n                            </div>    \n                            <div class=\"item-title\">\n                                <div>XML Errors</div>\n                            </div>                                                   \n                            <div class=\"item-caret\">\n                                <i class=\"fas\"></i>\n                            </div>                                                                                     \n                        </a> \n                        <ul class=\"dropdown-menu\">\n"
    + ((stack1 = lookupProperty(helpers,"each").call(depth0 != null ? depth0 : (container.nullContext || {}),(depth0 != null ? lookupProperty(depth0,"validation") : depth0),{"name":"each","hash":{},"fn":container.program(21, data, 0),"inverse":container.noop,"data":data,"loc":{"start":{"line":190,"column":28},"end":{"line":200,"column":37}}})) != null ? stack1 : "")
    + "                        </ul>                   \n                    </li>                \n";
},"21":function(container,depth0,helpers,partials,data) {
    return "                                <li>\n                                    <a class=\"dropdown-node\" href=\"#\">\n                                        <div class=\"item-icon\">\n                                            <i class=\"fas fa-exclamation-triangle text-danger\"></i>\n                                        </div>\n                                        <div class=\"item-title\">"
    + container.escapeExpression(container.lambda(depth0, depth0))
    + "</div>\n                                        <div class=\"item-caret\"></div>\n                                    </a>\n                                </li>                            \n";
},"23":function(container,depth0,helpers,partials,data) {
    var stack1, alias1=depth0 != null ? depth0 : (container.nullContext || {}), lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "    <nav class=\"navbar navbar-dropdown navbar-light navbar-rounded navbar-outline\">\n        <div class=\"btn-group dropstart\">\n            <button \n                class=\"btn btn-default dropdown-toggle "
    + ((stack1 = lookupProperty(helpers,"unless").call(alias1,((stack1 = (depth0 != null ? lookupProperty(depth0,"codes") : depth0)) != null ? lookupProperty(stack1,"length") : stack1),{"name":"unless","hash":{},"fn":container.program(24, data, 0),"inverse":container.noop,"data":data,"loc":{"start":{"line":214,"column":55},"end":{"line":214,"column":98}}})) != null ? stack1 : "")
    + "\" \n                type=\"button\" \n                data-mdb-toggle=\"dropdown\"\n                title=\"Access codes obtained from metered data providers\"\n            >\n                <div class=\"dropdown-icon\">              \n                    <span \n                        class=\"\n                            badge \n"
    + ((stack1 = lookupProperty(helpers,"unless").call(alias1,((stack1 = (depth0 != null ? lookupProperty(depth0,"codes") : depth0)) != null ? lookupProperty(stack1,"length") : stack1),{"name":"unless","hash":{},"fn":container.program(26, data, 0),"inverse":container.program(28, data, 0),"data":data,"loc":{"start":{"line":223,"column":28},"end":{"line":227,"column":39}}})) != null ? stack1 : "")
    + "                                rounded-pill\n                        \">"
    + container.escapeExpression(container.lambda(((stack1 = (depth0 != null ? lookupProperty(depth0,"codes") : depth0)) != null ? lookupProperty(stack1,"length") : stack1), depth0))
    + "\n                    </span>                                      \n                </div>\n                <div class=\"dropdown-title\" >Access Codes</div>\n                <div class=\"dropdown-caret\">\n                    <i class=\"fas\"></i>\n                </div>\n            </button>\n"
    + ((stack1 = lookupProperty(helpers,"if").call(alias1,((stack1 = (depth0 != null ? lookupProperty(depth0,"codes") : depth0)) != null ? lookupProperty(stack1,"length") : stack1),{"name":"if","hash":{},"fn":container.program(30, data, 0),"inverse":container.noop,"data":data,"loc":{"start":{"line":237,"column":12},"end":{"line":263,"column":19}}})) != null ? stack1 : "")
    + "        </div>\n    </nav>\n";
},"24":function(container,depth0,helpers,partials,data) {
    return "disabled";
},"26":function(container,depth0,helpers,partials,data) {
    return "                                badge-light\n";
},"28":function(container,depth0,helpers,partials,data) {
    return "                                badge-secondary\n";
},"30":function(container,depth0,helpers,partials,data) {
    var stack1, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "                <ul class=\"dropdown-menu\">\n"
    + ((stack1 = lookupProperty(helpers,"each").call(depth0 != null ? depth0 : (container.nullContext || {}),(depth0 != null ? lookupProperty(depth0,"codes") : depth0),{"name":"each","hash":{},"fn":container.program(31, data, 0),"inverse":container.noop,"data":data,"loc":{"start":{"line":239,"column":20},"end":{"line":261,"column":29}}})) != null ? stack1 : "")
    + "                </ul>            \n";
},"31":function(container,depth0,helpers,partials,data) {
    var stack1, helper, alias1=container.lambda, alias2=container.escapeExpression, alias3=depth0 != null ? depth0 : (container.nullContext || {}), alias4=container.hooks.helperMissing, alias5="function", lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "                        <li>\n                            <a \n                                class=\"dropdown-node\" \n                                title=\"Access code file. Located at gs://"
    + alias2(alias1(((stack1 = (depth0 != null ? lookupProperty(depth0,"cfg") : depth0)) != null ? lookupProperty(stack1,"bucket") : stack1), depth0))
    + "/"
    + alias2(((helper = (helper = lookupProperty(helpers,"name") || (depth0 != null ? lookupProperty(depth0,"name") : depth0)) != null ? helper : alias4),(typeof helper === alias5 ? helper.call(alias3,{"name":"name","hash":{},"data":data,"loc":{"start":{"line":244,"column":88},"end":{"line":244,"column":96}}}) : helper)))
    + "\"                \n                                data-bucket=\""
    + alias2(alias1(((stack1 = (depth0 != null ? lookupProperty(depth0,"cfg") : depth0)) != null ? lookupProperty(stack1,"bucket") : stack1), depth0))
    + "\"\n                                data-path=\""
    + alias2(((helper = (helper = lookupProperty(helpers,"name") || (depth0 != null ? lookupProperty(depth0,"name") : depth0)) != null ? helper : alias4),(typeof helper === alias5 ? helper.call(alias3,{"name":"name","hash":{},"data":data,"loc":{"start":{"line":246,"column":43},"end":{"line":246,"column":51}}}) : helper)))
    + "\"\n                                onclick=\"download_from_bucket(event)\"                               \n                                href=\"#\"\n                            >\n                                <div class=\"item-icon\">\n                                    <i class=\"bi bi-download\"></i>\n                                </div>\n                                <div class=\"item-title\">\n                                    "
    + alias2(((helper = (helper = lookupProperty(helpers,"baseName") || (depth0 != null ? lookupProperty(depth0,"baseName") : depth0)) != null ? helper : alias4),(typeof helper === alias5 ? helper.call(alias3,{"name":"baseName","hash":{},"data":data,"loc":{"start":{"line":256,"column":36},"end":{"line":256,"column":48}}}) : helper)))
    + "\n                                </div>\n                                <div class=\"item-caret\"></div>                                          \n                            </a>\n                        </li>                    \n";
},"33":function(container,depth0,helpers,partials,data) {
    var helper, alias1=depth0 != null ? depth0 : (container.nullContext || {}), alias2=container.hooks.helperMissing, alias3="function", alias4=container.escapeExpression, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "    <a\n        class=\"text-"
    + alias4(((helper = (helper = lookupProperty(helpers,"color") || (depth0 != null ? lookupProperty(depth0,"color") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"color","hash":{},"data":data,"loc":{"start":{"line":271,"column":20},"end":{"line":271,"column":29}}}) : helper)))
    + "\"\n        data-mdb-toggle=\"collapse\"\n        href=\"#collapse_"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":273,"column":24},"end":{"line":273,"column":31}}}) : helper)))
    + "\"\n        role=\"button\"\n        aria-expanded=\"true\"\n        aria-controls=\"collapseExample\"\n    >\n        <i class=\"fa-solid\"></i>\n    </a>  \n";
},"35":function(container,depth0,helpers,partials,data) {
    return "    <ul class=\"list-group list-group-light list-files mb-2 ms-3 me-3\">\n        <li class=\"list-group-item d-flex flex-row justify-content-center align-items-center\">\n            <div class=\"flex-shrink-0\">\n                <div class=\"ps-3 pe-3 badge-danger\">\n                    <i class=\"fas fa-exclamation-circle\"></i>\n                </div>\n            </div>   \n            <div class=\"flex-grow-1 ms-4\">\n                <p class=\"fw-bold mb-1\">Files are not uploaded</p>\n            </div>\n            \n        </li>                       \n    </ul>\n";
},"37":function(container,depth0,helpers,partials,data) {
    var stack1, helper, alias1=depth0 != null ? depth0 : (container.nullContext || {}), alias2=container.hooks.helperMissing, alias3="function", alias4=container.escapeExpression, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "    <div class=\"card border border-secondary shadow-0 rounded-0 mb-1\">\n        <div class=\"card-header bg-light pt-0 pb-0 ps-1 pe-1\">\n            <div class=\"d-flex flex-row justify-content-between\">\n                <div class=\"d-flex flex-row justify-content-start flex-grow-1 justify-content-between align-items-center ms-2\">\n                    <div class=\"d-flex align-items-center flex-row ps-1 pe-1 meter-title\">\n                        <div>\n                            <iconify-icon icon=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"icon_type") || (depth0 != null ? lookupProperty(depth0,"icon_type") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"icon_type","hash":{},"data":data,"loc":{"start":{"line":306,"column":48},"end":{"line":306,"column":61}}}) : helper)))
    + "\" class =\"fs-3\"></iconify-icon>\n                        </div>\n                        <div class=\"\" title=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"type") || (depth0 != null ? lookupProperty(depth0,"type") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"type","hash":{},"data":data,"loc":{"start":{"line":308,"column":45},"end":{"line":308,"column":53}}}) : helper)))
    + "\">\n                            "
    + alias4(((helper = (helper = lookupProperty(helpers,"short_uri") || (depth0 != null ? lookupProperty(depth0,"short_uri") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"short_uri","hash":{},"data":data,"loc":{"start":{"line":309,"column":28},"end":{"line":309,"column":41}}}) : helper)))
    + "\n                        </div>\n                    </div>    \n                    <div class=\"d-flex flex-row align-items-center m-2\">\n                        <div class=\"badge badge-dark rounded-0\">Last Updated:</div>\n"
    + ((stack1 = lookupProperty(helpers,"if").call(alias1,(depth0 != null ? lookupProperty(depth0,"lastUpdateHoursHumanized") : depth0),{"name":"if","hash":{},"fn":container.program(38, data, 0),"inverse":container.program(40, data, 0),"data":data,"loc":{"start":{"line":314,"column":24},"end":{"line":318,"column":31}}})) != null ? stack1 : "")
    + "                    </div>\n                </div>\n                <div class=\"d-flex flex-row justify-content-start align-items-center ms-4 me-2\">\n"
    + ((stack1 = container.invokePartial(lookupProperty(partials,"CardToggler"),depth0,{"name":"CardToggler","hash":{"color":"dark"},"data":data,"indent":"                    ","helpers":helpers,"partials":partials,"decorators":container.decorators})) != null ? stack1 : "")
    + "                </div>\n            </div>\n        </div>\n        <div class=\"card-body collapse\" id=\"collapse_"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":326,"column":53},"end":{"line":326,"column":60}}}) : helper)))
    + "\">\n            <div class=\"d-flex flex-row flex-grow-1 justify-content-start meter-detail-container\">\n                <section class=\"d-flex flex-column meter-section\">\n                    <h6 class=\"bg-light p-2 border-top border-bottom text-center\">Meter General Information</h6>\n                    <ul class=\"list-group list-group-light mb-2\">\n                        <li class=\"list-group-item d-flex justify-content-between align-items-center\">\n                            Type:\n                            <span class=\"badge badge-secondary rounded-0\">"
    + alias4(((helper = (helper = lookupProperty(helpers,"type") || (depth0 != null ? lookupProperty(depth0,"type") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"type","hash":{},"data":data,"loc":{"start":{"line":333,"column":74},"end":{"line":333,"column":82}}}) : helper)))
    + "</span>                            \n                        </li>\n                        <li class=\"list-group-item d-flex justify-content-between align-items-center\">\n                            Exp. updates:\n                            <span class=\"badge badge-secondary rounded-0\">"
    + alias4(((helper = (helper = lookupProperty(helpers,"updateFrequency") || (depth0 != null ? lookupProperty(depth0,"updateFrequency") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"updateFrequency","hash":{},"data":data,"loc":{"start":{"line":337,"column":74},"end":{"line":337,"column":93}}}) : helper)))
    + "</span>                               \n                        </li>\n                        <li class=\"list-group-item d-flex justify-content-between align-items-center\">\n                            Last Updated:\n"
    + ((stack1 = lookupProperty(helpers,"if").call(alias1,(depth0 != null ? lookupProperty(depth0,"lastUpdateHoursHumanized") : depth0),{"name":"if","hash":{},"fn":container.program(42, data, 0),"inverse":container.program(44, data, 0),"data":data,"loc":{"start":{"line":341,"column":28},"end":{"line":345,"column":35}}})) != null ? stack1 : "")
    + "                        </li>       \n                        <li class=\"list-group-item d-flex justify-content-between align-items-center\">\n                            Haystack:\n                            <span class=\"badge badge-secondary rounded-0\">"
    + alias4(((helper = (helper = lookupProperty(helpers,"haystack") || (depth0 != null ? lookupProperty(depth0,"haystack") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"haystack","hash":{},"data":data,"loc":{"start":{"line":349,"column":74},"end":{"line":349,"column":86}}}) : helper)))
    + "</span>                                  \n                        </li>         \n                        <li class=\"list-group-item d-flex justify-content-between align-items-center\">\n                            Problems:\n"
    + ((stack1 = lookupProperty(helpers,"unless").call(alias1,(depth0 != null ? lookupProperty(depth0,"validation") : depth0),{"name":"unless","hash":{},"fn":container.program(46, data, 0),"inverse":container.program(48, data, 0),"data":data,"loc":{"start":{"line":353,"column":28},"end":{"line":357,"column":39}}})) != null ? stack1 : "")
    + "                                                              \n                        </li>\n                    </ul>\n                </section>\n                <div class=\"vr vr-blurry align-self-stretch\"></div>\n                <section class=\"d-flex flex-column flex-grow-1 justify-content-start meter-section\">\n                    <h6 class=\"bg-light p-2 border-top border-bottom text-center\">Meter Actions</h6>\n                    <ul class=\"list-group list-group-light mb-2 ms-3 me-3\">\n                        <li class=\"list-group-item d-flex flex-column justify-content-center align-items-center p-1\">\n                            <div class=\"flex-shrink-0 mb-2 ms-2\">\n                                <a \n                                    class=\"btn btn-lg btn-link\" \n                                    title=\"Access code file. Located at gs://"
    + alias4(((helper = (helper = lookupProperty(helpers,"bucket") || (depth0 != null ? lookupProperty(depth0,"bucket") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"bucket","hash":{},"data":data,"loc":{"start":{"line":370,"column":77},"end":{"line":370,"column":87}}}) : helper)))
    + "/"
    + alias4(((helper = (helper = lookupProperty(helpers,"fileName") || (depth0 != null ? lookupProperty(depth0,"fileName") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"fileName","hash":{},"data":data,"loc":{"start":{"line":370,"column":88},"end":{"line":370,"column":100}}}) : helper)))
    + "\"                \n                                    data-bucket=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"bucket") || (depth0 != null ? lookupProperty(depth0,"bucket") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"bucket","hash":{},"data":data,"loc":{"start":{"line":371,"column":49},"end":{"line":371,"column":59}}}) : helper)))
    + "\"\n                                    data-path=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"fileName") || (depth0 != null ? lookupProperty(depth0,"fileName") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"fileName","hash":{},"data":data,"loc":{"start":{"line":372,"column":47},"end":{"line":372,"column":59}}}) : helper)))
    + "\"\n                                    onclick=\"download_from_bucket(event)\"                               \n                                    href=\"#\"\n                                >\n                                    Download Meter Config                                                                \n                                </a> \n                            </div>                            \n                        </li>\n                        \n                        <li class=\"list-group-item d-flex flex-column justify-content-center align-items-center p-1\">\n                            <div class=\"mb-2 ms-2\">\n                                <label class=\"btn btn-lg btn-link\" for=\"meter_config_upload_label_"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":383,"column":98},"end":{"line":383,"column":105}}}) : helper)))
    + "\">Update Meter Config</label>\n                                <input type=\"file\"\n                                    id=\"meter_config_upload_label_"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":385,"column":66},"end":{"line":385,"column":73}}}) : helper)))
    + "\"\n                                    style=\"display: none;\"\n                                    data-status=\"meter_cfg_upload_status_"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":387,"column":73},"end":{"line":387,"column":80}}}) : helper)))
    + "\"\n                                    data-bucket=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"bucket") || (depth0 != null ? lookupProperty(depth0,"bucket") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"bucket","hash":{},"data":data,"loc":{"start":{"line":388,"column":49},"end":{"line":388,"column":59}}}) : helper)))
    + "\"\n                                    data-path=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"fileName") || (depth0 != null ? lookupProperty(depth0,"fileName") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"fileName","hash":{},"data":data,"loc":{"start":{"line":389,"column":47},"end":{"line":389,"column":59}}}) : helper)))
    + "\"\n                                    onchange='uploadToStorageBucket(event)'\n                                    multiple />\n                                <span id=\"meter_cfg_upload_status_"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":392,"column":66},"end":{"line":392,"column":73}}}) : helper)))
    + "\" class=\"message\"></span>                                    \n                            </div>\n\n                        </li>\n                    </ul>                    \n                </section>                \n                <div class=\"vr vr-blurry align-self-stretch\"></div>\n                <section class=\"d-flex flex-column flex-grow-1 justify-content-start meter-section\">\n                    <h6 class=\"bg-light p-2 border-top border-bottom text-center\">Meter Latest Data Files</h6>\n"
    + ((stack1 = lookupProperty(helpers,"if").call(alias1,((stack1 = (depth0 != null ? lookupProperty(depth0,"stdFiles") : depth0)) != null ? lookupProperty(stack1,"length") : stack1),{"name":"if","hash":{},"fn":container.program(50, data, 0),"inverse":container.program(53, data, 0),"data":data,"loc":{"start":{"line":401,"column":20},"end":{"line":428,"column":27}}})) != null ? stack1 : "")
    + "                </section>\n                <div class=\"vr vr-blurry align-self-stretch\"></div>\n            </div>\n        </div>\n    </div>\n";
},"38":function(container,depth0,helpers,partials,data) {
    var helper, alias1=depth0 != null ? depth0 : (container.nullContext || {}), alias2=container.hooks.helperMissing, alias3="function", alias4=container.escapeExpression, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "                            <div class=\"badge badge-"
    + alias4(((helper = (helper = lookupProperty(helpers,"status") || (depth0 != null ? lookupProperty(depth0,"status") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"status","hash":{},"data":data,"loc":{"start":{"line":315,"column":52},"end":{"line":315,"column":62}}}) : helper)))
    + " rounded-0\">"
    + alias4(((helper = (helper = lookupProperty(helpers,"lastUpdateHoursHumanized") || (depth0 != null ? lookupProperty(depth0,"lastUpdateHoursHumanized") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"lastUpdateHoursHumanized","hash":{},"data":data,"loc":{"start":{"line":315,"column":74},"end":{"line":315,"column":102}}}) : helper)))
    + " ago</div>\n";
},"40":function(container,depth0,helpers,partials,data) {
    return "                            <div class=\"badge badge-danger rounded-0\">Not Available</div>\n";
},"42":function(container,depth0,helpers,partials,data) {
    var helper, alias1=depth0 != null ? depth0 : (container.nullContext || {}), alias2=container.hooks.helperMissing, alias3="function", alias4=container.escapeExpression, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "                                <span class=\"badge badge-"
    + alias4(((helper = (helper = lookupProperty(helpers,"status") || (depth0 != null ? lookupProperty(depth0,"status") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"status","hash":{},"data":data,"loc":{"start":{"line":342,"column":57},"end":{"line":342,"column":67}}}) : helper)))
    + " rounded-0\">"
    + alias4(((helper = (helper = lookupProperty(helpers,"lastUpdateHoursHumanized") || (depth0 != null ? lookupProperty(depth0,"lastUpdateHoursHumanized") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"lastUpdateHoursHumanized","hash":{},"data":data,"loc":{"start":{"line":342,"column":79},"end":{"line":342,"column":107}}}) : helper)))
    + " ago</span>\n";
},"44":function(container,depth0,helpers,partials,data) {
    return "                                <span class=\"badge badge-danger rounded-0\">Not Available</span>\n";
},"46":function(container,depth0,helpers,partials,data) {
    var stack1, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "                                <span class=\"badge badge-success rounded-0\">"
    + container.escapeExpression(container.lambda(((stack1 = (depth0 != null ? lookupProperty(depth0,"validation") : depth0)) != null ? lookupProperty(stack1,"length") : stack1), depth0))
    + "</span>\n";
},"48":function(container,depth0,helpers,partials,data) {
    var stack1, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "                                <span class=\"badge badge-warning rounded-0\">"
    + container.escapeExpression(container.lambda(((stack1 = (depth0 != null ? lookupProperty(depth0,"validation") : depth0)) != null ? lookupProperty(stack1,"length") : stack1), depth0))
    + "</span>\n";
},"50":function(container,depth0,helpers,partials,data) {
    var stack1, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "                        <ul class=\"list-group list-group-light list-files mb-2 ms-3 me-3\">\n"
    + ((stack1 = lookupProperty(helpers,"each").call(depth0 != null ? depth0 : (container.nullContext || {}),(depth0 != null ? lookupProperty(depth0,"stdFiles") : depth0),{"name":"each","hash":{},"fn":container.program(51, data, 0),"inverse":container.noop,"data":data,"loc":{"start":{"line":403,"column":28},"end":{"line":424,"column":37}}})) != null ? stack1 : "")
    + "                        </ul>\n";
},"51":function(container,depth0,helpers,partials,data) {
    var helper, alias1=depth0 != null ? depth0 : (container.nullContext || {}), alias2=container.hooks.helperMissing, alias3="function", alias4=container.escapeExpression, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "                                <li class=\"list-group-item d-flex flex-row justify-content-start align-items-center\">\n                                    <div class=\"flex-shrink-0 ps-2 pe-2\">\n                                       <i class=\"fs-2 bi bi-filetype-xml\"></i>\n                                    </div>  \n                                    <div class=\"flex-grow-1 ps-2 pe-2 fs-1 fw-bold text-center\">\n                                        "
    + alias4(((helper = (helper = lookupProperty(helpers,"baseName") || (depth0 != null ? lookupProperty(depth0,"baseName") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"baseName","hash":{},"data":data,"loc":{"start":{"line":409,"column":40},"end":{"line":409,"column":52}}}) : helper)))
    + "\n                                    </div> \n                                    <div class=\"flex-shrink-0 ps-2 pe-2\">\n                                        <a \n                                            class=\"dropdown-node\" \n                                            title=\"Access code file. Located at gs://"
    + alias4(((helper = (helper = lookupProperty(helpers,"bucket") || (depth0 != null ? lookupProperty(depth0,"bucket") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"bucket","hash":{},"data":data,"loc":{"start":{"line":414,"column":85},"end":{"line":414,"column":95}}}) : helper)))
    + "/"
    + alias4(((helper = (helper = lookupProperty(helpers,"name") || (depth0 != null ? lookupProperty(depth0,"name") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"name","hash":{},"data":data,"loc":{"start":{"line":414,"column":96},"end":{"line":414,"column":104}}}) : helper)))
    + "\"                \n                                            data-bucket=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"bucket") || (depth0 != null ? lookupProperty(depth0,"bucket") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"bucket","hash":{},"data":data,"loc":{"start":{"line":415,"column":57},"end":{"line":415,"column":67}}}) : helper)))
    + "\"\n                                            data-path=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"name") || (depth0 != null ? lookupProperty(depth0,"name") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"name","hash":{},"data":data,"loc":{"start":{"line":416,"column":55},"end":{"line":416,"column":63}}}) : helper)))
    + "\"\n                                            onclick=\"download_from_bucket(event)\"                               \n                                            href=\"#\"\n                                        >\n                                            Download                                                                     \n                                        </a> \n                                    </div>\n                                </li>\n";
},"53":function(container,depth0,helpers,partials,data) {
    var stack1, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return ((stack1 = container.invokePartial(lookupProperty(partials,"FilesNotLoaaded"),depth0,{"name":"FilesNotLoaaded","data":data,"indent":"                        ","helpers":helpers,"partials":partials,"decorators":container.decorators})) != null ? stack1 : "");
},"55":function(container,depth0,helpers,partials,data) {
    var stack1, helper, alias1=depth0 != null ? depth0 : (container.nullContext || {}), alias2=container.hooks.helperMissing, alias3="function", alias4=container.escapeExpression, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "    <div class=\"card border border-secondary shadow-0 rounded-0 mb-1\">\n        <div class=\"card-header bg-light pt-0 pb-0 ps-1 pe-1 rounded-0\">\n            <div class=\"d-flex flex-row justify-content-between\">\n                <div class=\"d-flex flex-row justify-content-start flex-grow-1 justify-content-between align-items-center ms-2\">\n                    <div class=\"d-flex align-items-center flex-row meter-title\">\n                        <div class=\"me-2\">\n                            <i class=\"fa fa-upload\" aria-hidden=\"true\"></i>\n                        </div>\n                        <div class=\"ms-2 me-2\" title=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"description") || (depth0 != null ? lookupProperty(depth0,"description") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"description","hash":{},"data":data,"loc":{"start":{"line":446,"column":54},"end":{"line":446,"column":69}}}) : helper)))
    + "\">\n                            "
    + alias4(((helper = (helper = lookupProperty(helpers,"connectorName") || (depth0 != null ? lookupProperty(depth0,"connectorName") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"connectorName","hash":{},"data":data,"loc":{"start":{"line":447,"column":28},"end":{"line":447,"column":45}}}) : helper)))
    + "\n                        </div>\n                    </div>    \n                </div>\n                <div class=\"d-flex flex-row justify-content-start align-items-center ms-4 me-2\">\n"
    + ((stack1 = container.invokePartial(lookupProperty(partials,"CardToggler"),depth0,{"name":"CardToggler","hash":{"color":"dark"},"data":data,"indent":"                    ","helpers":helpers,"partials":partials,"decorators":container.decorators})) != null ? stack1 : "")
    + "                </div>\n            </div>\n        </div>\n        <div class=\"card-body collapse\" id=\"collapse_"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":456,"column":53},"end":{"line":456,"column":60}}}) : helper)))
    + "\">\n            <div class=\"d-flex flex-row flex-grow-1 justify-content-start meter-pushes-container\">\n                <section class=\"d-flex flex-column ms-2 meter-pushes-section\">\n                    <h6 class=\"bg-light p-2 border-top border-bottom text-center\">Meter Information</h6>\n                    <ul class=\"list-group list-group-light mb-2\">\n"
    + ((stack1 = lookupProperty(helpers,"each").call(alias1,(depth0 != null ? lookupProperty(depth0,"meterURIs") : depth0),{"name":"each","hash":{},"fn":container.program(56, data, 0),"inverse":container.noop,"data":data,"loc":{"start":{"line":461,"column":24},"end":{"line":473,"column":33}}})) != null ? stack1 : "")
    + "                    </ul>                \n                </section>\n                <div class=\"vr vr-blurry align-self-stretch\"></div>\n                <section class=\"d-flex flex-column flex-grow-1 justify-content-start meter-pushes-section\">\n                    <h6 class=\"bg-light p-2 border-top border-bottom text-center\">Previous Uploads</h6>\n"
    + ((stack1 = lookupProperty(helpers,"if").call(alias1,((stack1 = (depth0 != null ? lookupProperty(depth0,"uploads") : depth0)) != null ? lookupProperty(stack1,"length") : stack1),{"name":"if","hash":{},"fn":container.program(58, data, 0),"inverse":container.program(53, data, 0),"data":data,"loc":{"start":{"line":479,"column":24},"end":{"line":505,"column":27}}})) != null ? stack1 : "")
    + "                </section>                \n                <div class=\"vr vr-blurry align-self-stretch\"></div>\n                <section class=\"d-flex flex-column flex-shrink-0 justify-content-start meter-pushes-section meter-description-pushes-section\">\n                    <div class=\"d-flex flex-column\">\n                        <h6 class=\"bg-light p-2 border-top border-bottom text-center\">Description</h6>\n                        <ul class=\"list-group list-group-light mb-2 ms-3 me-3\">\n                            <li class=\"list-group-item d-flex flex-row justify-content-start align-items-center\">\n                                <div class=\"text-truncate\" title=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"description") || (depth0 != null ? lookupProperty(depth0,"description") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"description","hash":{},"data":data,"loc":{"start":{"line":513,"column":66},"end":{"line":513,"column":81}}}) : helper)))
    + "\">"
    + alias4(((helper = (helper = lookupProperty(helpers,"description") || (depth0 != null ? lookupProperty(depth0,"description") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"description","hash":{},"data":data,"loc":{"start":{"line":513,"column":83},"end":{"line":513,"column":98}}}) : helper)))
    + "</div>\n                            </li>\n                        </ul>\n                    </div>\n                    <div class=\"d-flex flex-column\">\n                        <h6 class=\"bg-light p-2 border-top border-bottom text-center\">Upload Meters Data</h6>\n                        <ul class=\"list-group list-group-light mb-2 ms-3 me-3\">\n                            <li class=\"list-group-item d-flex flex-column justify-content-start align-items-center p-1\">\n                                <div class=\"mb-2 ms-2\">\n                                    <label class=\"btn btn-lg btn-link\" for=\"push_upload_label_"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":522,"column":94},"end":{"line":522,"column":101}}}) : helper)))
    + "\">Add Metered Data</label>\n                                    <input type=\"file\"\n                                        id=\"push_upload_label_"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":524,"column":62},"end":{"line":524,"column":69}}}) : helper)))
    + "\"\n                                        style=\"display: none;\"\n                                        data-status=\"push_upload_status_"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":526,"column":72},"end":{"line":526,"column":79}}}) : helper)))
    + "\"\n                                        data-bucket=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"bucket") || (depth0 != null ? lookupProperty(depth0,"bucket") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"bucket","hash":{},"data":data,"loc":{"start":{"line":527,"column":53},"end":{"line":527,"column":63}}}) : helper)))
    + "\"\n                                        data-path=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"fileName") || (depth0 != null ? lookupProperty(depth0,"fileName") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"fileName","hash":{},"data":data,"loc":{"start":{"line":528,"column":51},"end":{"line":528,"column":63}}}) : helper)))
    + "\"\n                                        onchange='uploadToStorageBucket(event)'\n                                        multiple />\n                                    <span id=\"push_upload_status_"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":531,"column":65},"end":{"line":531,"column":72}}}) : helper)))
    + "\" class=\"message\"></span>                                    \n                                </div>\n\n                                <div class=\"flex-shrink-1\">\n                                    <div class=\"note note-info p-1\">\n                                        <p class=\"mb-0\">\n                                            One or more files can be selected, to appear named like this: raw-data-0-"
    + alias4(container.lambda(((stack1 = (data && lookupProperty(data,"root"))) && lookupProperty(stack1,"now")), depth0))
    + " ...\n                                        </p>\n                                    </div>\n                                </div>\n                            </li>\n                        </ul>                        \n                    </div>\n                </section>\n            </div>\n        </div>\n    </div>\n";
},"56":function(container,depth0,helpers,partials,data) {
    var helper, alias1=depth0 != null ? depth0 : (container.nullContext || {}), alias2=container.hooks.helperMissing, alias3="function", alias4=container.escapeExpression, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "                            <li class=\"list-group-item d-flex justify-content-between align-items-center\">\n                                <div class=\"ms-2 me-2\">\n                                    <iconify-icon icon=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"icon_type") || (depth0 != null ? lookupProperty(depth0,"icon_type") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"icon_type","hash":{},"data":data,"loc":{"start":{"line":464,"column":56},"end":{"line":464,"column":69}}}) : helper)))
    + "\" class =\"fs-3\"></iconify-icon>                                    \n                                </div>\n                                <div class=\"ms-2 me-2\">\n                                    <span>"
    + alias4(((helper = (helper = lookupProperty(helpers,"meterUri") || (depth0 != null ? lookupProperty(depth0,"meterUri") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"meterUri","hash":{},"data":data,"loc":{"start":{"line":467,"column":42},"end":{"line":467,"column":54}}}) : helper)))
    + "</span>\n                                </div>\n                                <div class=\"ms-2 me-2\">\n                                    <span class=\"fs-1 badge badge-secondary rounded-0\">"
    + alias4(((helper = (helper = lookupProperty(helpers,"type") || (depth0 != null ? lookupProperty(depth0,"type") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"type","hash":{},"data":data,"loc":{"start":{"line":470,"column":87},"end":{"line":470,"column":95}}}) : helper)))
    + "</span>\n                                </div>\n                            </li>\n";
},"58":function(container,depth0,helpers,partials,data) {
    var stack1, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "                        <ul class=\"list-group list-group-light list-files mb-2 ms-3 me-3\">\n"
    + ((stack1 = lookupProperty(helpers,"each").call(depth0 != null ? depth0 : (container.nullContext || {}),(depth0 != null ? lookupProperty(depth0,"uploads") : depth0),{"name":"each","hash":{},"fn":container.program(59, data, 0),"inverse":container.noop,"data":data,"loc":{"start":{"line":481,"column":28},"end":{"line":501,"column":37}}})) != null ? stack1 : "")
    + "                        </ul>\n";
},"59":function(container,depth0,helpers,partials,data) {
    var helper, alias1=container.lambda, alias2=container.escapeExpression, alias3=depth0 != null ? depth0 : (container.nullContext || {}), alias4=container.hooks.helperMissing, alias5="function", lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "                                <li class=\"list-group-item d-flex flex-row justify-content-start align-items-center\">\n                                    <div class=\"flex-shrink-0 ps-2 pe-2\">\n                                       <i class=\"fs-2 bi bi-filetype-xml\"></i>\n                                    </div>  \n                                    <div class=\"flex-grow-1 ps-2 pe-2 fs-1 fw-bold text-center\">\n                                        "
    + alias2(alias1(depth0, depth0))
    + "\n                                    </div> \n                                    <div class=\"flex-shrink-0 ps-2 pe-2\">\n                                        <a \n                                            title=\"The "
    + alias2(((helper = (helper = lookupProperty(helpers,"meterUri") || (depth0 != null ? lookupProperty(depth0,"meterUri") : depth0)) != null ? helper : alias4),(typeof helper === alias5 ? helper.call(alias3,{"name":"meterUri","hash":{},"data":data,"loc":{"start":{"line":491,"column":55},"end":{"line":491,"column":67}}}) : helper)))
    + " raw data file. Located at gs://"
    + alias2(((helper = (helper = lookupProperty(helpers,"bucket") || (depth0 != null ? lookupProperty(depth0,"bucket") : depth0)) != null ? helper : alias4),(typeof helper === alias5 ? helper.call(alias3,{"name":"bucket","hash":{},"data":data,"loc":{"start":{"line":491,"column":99},"end":{"line":491,"column":109}}}) : helper)))
    + "/"
    + alias2(((helper = (helper = lookupProperty(helpers,"fileName") || (depth0 != null ? lookupProperty(depth0,"fileName") : depth0)) != null ? helper : alias4),(typeof helper === alias5 ? helper.call(alias3,{"name":"fileName","hash":{},"data":data,"loc":{"start":{"line":491,"column":110},"end":{"line":491,"column":122}}}) : helper)))
    + "\"                \n                                            data-bucket=\""
    + alias2(((helper = (helper = lookupProperty(helpers,"bucket") || (depth0 != null ? lookupProperty(depth0,"bucket") : depth0)) != null ? helper : alias4),(typeof helper === alias5 ? helper.call(alias3,{"name":"bucket","hash":{},"data":data,"loc":{"start":{"line":492,"column":57},"end":{"line":492,"column":67}}}) : helper)))
    + "\"\n                                            data-path=\""
    + alias2(((helper = (helper = lookupProperty(helpers,"fileName") || (depth0 != null ? lookupProperty(depth0,"fileName") : depth0)) != null ? helper : alias4),(typeof helper === alias5 ? helper.call(alias3,{"name":"fileName","hash":{},"data":data,"loc":{"start":{"line":493,"column":55},"end":{"line":493,"column":67}}}) : helper)))
    + "/"
    + alias2(alias1(depth0, depth0))
    + "\"\n                                            onclick=\"download_from_bucket(event)\"                               \n                                            href=\"#\"\n                                        >\n                                            Download                                                                     \n                                        </a> \n                                    </div>\n                                </li>\n";
},"61":function(container,depth0,helpers,partials,data) {
    var stack1, helper, alias1=depth0 != null ? depth0 : (container.nullContext || {}), alias2=container.hooks.helperMissing, alias3="function", alias4=container.escapeExpression, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "    <div class=\"card border border-secondary shadow-0 rounded-0 mb-1\">\n        <div class=\"card-header bg-light pt-0 pb-0 ps-1 pe-1 rounded-0\">\n            <div class=\"d-flex flex-row justify-content-between\">\n                <div class=\"d-flex flex-row justify-content-start flex-grow-1 justify-content-between align-items-center ms-2\">\n                    <div class=\"d-flex align-items-center flex-row meter-title\">\n                        <div class=\"me-2 mt-2\">\n                            <i class=\"fs-2 fas fa-building\" aria-hidden=\"true\"></i>\n                        </div>\n                        <div class=\"ms-2 me-2\" title=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"address") || (depth0 != null ? lookupProperty(depth0,"address") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"address","hash":{},"data":data,"loc":{"start":{"line":560,"column":54},"end":{"line":560,"column":65}}}) : helper)))
    + "\">\n                            "
    + alias4(((helper = (helper = lookupProperty(helpers,"name") || (depth0 != null ? lookupProperty(depth0,"name") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"name","hash":{},"data":data,"loc":{"start":{"line":561,"column":28},"end":{"line":561,"column":36}}}) : helper)))
    + "\n                        </div>\n                    </div>    \n                </div>\n                <div class=\"d-flex flex-row justify-content-start align-items-center ms-4 me-2\">\n"
    + ((stack1 = container.invokePartial(lookupProperty(partials,"CardToggler"),depth0,{"name":"CardToggler","hash":{"color":"dark"},"data":data,"indent":"                    ","helpers":helpers,"partials":partials,"decorators":container.decorators})) != null ? stack1 : "")
    + "                </div>\n            </div>\n        </div>\n        <div class=\"card-body collapse\" id=\"collapse_"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":570,"column":53},"end":{"line":570,"column":60}}}) : helper)))
    + "\">\n            <div class=\"d-flex flex-row flex-grow-1 align-items-stretch property-container\">\n                <section class=\"d-flex flex-column property-section flex-shrink-1\">\n                    <h6 class=\"bg-light p-2 border-top border-bottom text-center\">\n                        Property Information\n                    </h6>\n                    <ul class=\"list-group list-group-light mb-2 overflow-auto\">\n                        <li class=\"list-group-item d-flex flex-row justify-content-between align-items-center\">\n                            <div class=\"pe-1 flex-shrink-1\"><strong>Address:</strong></div>\n                            <div class=\"badge badge-secondary rounded-0 property-info-item\" title=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"address") || (depth0 != null ? lookupProperty(depth0,"address") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"address","hash":{},"data":data,"loc":{"start":{"line":579,"column":99},"end":{"line":579,"column":110}}}) : helper)))
    + "\">"
    + alias4(((helper = (helper = lookupProperty(helpers,"address") || (depth0 != null ? lookupProperty(depth0,"address") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"address","hash":{},"data":data,"loc":{"start":{"line":579,"column":112},"end":{"line":579,"column":123}}}) : helper)))
    + "</div>\n                        </li>\n\n                        <li class=\"list-group-item d-flex flex-row justify-content-between align-items-center\">\n                            <div class=\"pe-1 flex-shrink-1\"><strong>Bucket:</strong></div>                            \n                            <div class=\"ps-1 pe-1\">\n                                <a\n                                    class=\"btn btn-link p-0 property-info-item text-nowrap text-truncate text-primary\"\n                                    href=\"https://console.cloud.google.com/storage/browser/"
    + alias4(((helper = (helper = lookupProperty(helpers,"bucket") || (depth0 != null ? lookupProperty(depth0,"bucket") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"bucket","hash":{},"data":data,"loc":{"start":{"line":587,"column":91},"end":{"line":587,"column":101}}}) : helper)))
    + ";tab=objects?forceOnBucketsSortingFiltering=false&project="
    + alias4(((helper = (helper = lookupProperty(helpers,"env") || (depth0 != null ? lookupProperty(depth0,"env") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"env","hash":{},"data":data,"loc":{"start":{"line":587,"column":159},"end":{"line":587,"column":166}}}) : helper)))
    + "&prefix=&forceOnObjectsSortingFiltering=false\"\n                                    target=\"_blank\"\n                                    data-mdb-toggle=\"tooltip\" \n                                    data-mdb-placement=\"right\"\n                                    title=\"Property's Google Storage Bucket: gs://"
    + alias4(((helper = (helper = lookupProperty(helpers,"bucket") || (depth0 != null ? lookupProperty(depth0,"bucket") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"bucket","hash":{},"data":data,"loc":{"start":{"line":591,"column":82},"end":{"line":591,"column":92}}}) : helper)))
    + "\"\n                                >\n                                    "
    + alias4(((helper = (helper = lookupProperty(helpers,"bucket") || (depth0 != null ? lookupProperty(depth0,"bucket") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"bucket","hash":{},"data":data,"loc":{"start":{"line":593,"column":36},"end":{"line":593,"column":46}}}) : helper)))
    + "\n                                    \n                                </a>\n                            </div>  \n                            <div class=\"ms-2\">\n                                <i class=\"fs-1 fas fa-external-link-alt text-primary\"></i>\n                            </div>\n                        </li>\n\n                        <li class=\"list-group-item d-flex flex-row justify-content-between align-items-center\">\n                            <div class=\"pe-1\"><strong>Dashboard:</strong></div>\n                            <div class=\"ps-1 pe-1 flex-shrink-1\">\n                                <a\n                                    class=\"btn btn-link p-0 property-info-item text-nowrap text-truncate text-primary\"\n                                    title=\"The "
    + alias4(((helper = (helper = lookupProperty(helpers,"name") || (depth0 != null ? lookupProperty(depth0,"name") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"name","hash":{},"data":data,"loc":{"start":{"line":607,"column":47},"end":{"line":607,"column":55}}}) : helper)))
    + " property configuration file. Located at gs://"
    + alias4(((helper = (helper = lookupProperty(helpers,"bucket") || (depth0 != null ? lookupProperty(depth0,"bucket") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"bucket","hash":{},"data":data,"loc":{"start":{"line":607,"column":101},"end":{"line":607,"column":111}}}) : helper)))
    + "/"
    + alias4(((helper = (helper = lookupProperty(helpers,"fileName") || (depth0 != null ? lookupProperty(depth0,"fileName") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"fileName","hash":{},"data":data,"loc":{"start":{"line":607,"column":112},"end":{"line":607,"column":124}}}) : helper)))
    + "\"                \n                                    id=\"property_dashboard_button_"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":608,"column":66},"end":{"line":608,"column":73}}}) : helper)))
    + "\"\n                                    class=\"link-button\"\n                                    href="
    + alias4(((helper = (helper = lookupProperty(helpers,"dataStudioLink") || (depth0 != null ? lookupProperty(depth0,"dataStudioLink") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"dataStudioLink","hash":{},"data":data,"loc":{"start":{"line":610,"column":41},"end":{"line":610,"column":59}}}) : helper)))
    + "\n                                    target=\"_blank\"                               \n                                >\n                                    "
    + alias4(((helper = (helper = lookupProperty(helpers,"address") || (depth0 != null ? lookupProperty(depth0,"address") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"address","hash":{},"data":data,"loc":{"start":{"line":613,"column":36},"end":{"line":613,"column":47}}}) : helper)))
    + "                                    \n                                </a>\n                            </div>  \n                            <div class=\"ms-2\">\n                                <i class=\"fs-1 fas fa-external-link-alt text-primary\"></i>\n                            </div>                            \n                        </li>                          \n                        <li class=\"list-group-item d-flex flex-row justify-content-between align-items-center\">\n                            <div class=\"pe-1 flex-shrink-1\"><strong>Configuration:</strong></div>\n                            <div class=\"ps-1 pe-1 flex-shrink-2\">\n                                <a\n                                    class=\"btn btn-link p-0 property-info-item text-nowrap text-truncate text-primary\"\n                                    title=\"The "
    + alias4(((helper = (helper = lookupProperty(helpers,"name") || (depth0 != null ? lookupProperty(depth0,"name") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"name","hash":{},"data":data,"loc":{"start":{"line":625,"column":47},"end":{"line":625,"column":55}}}) : helper)))
    + " property configuration file. Located at gs://"
    + alias4(((helper = (helper = lookupProperty(helpers,"bucket") || (depth0 != null ? lookupProperty(depth0,"bucket") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"bucket","hash":{},"data":data,"loc":{"start":{"line":625,"column":101},"end":{"line":625,"column":111}}}) : helper)))
    + "/"
    + alias4(((helper = (helper = lookupProperty(helpers,"fileName") || (depth0 != null ? lookupProperty(depth0,"fileName") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"fileName","hash":{},"data":data,"loc":{"start":{"line":625,"column":112},"end":{"line":625,"column":124}}}) : helper)))
    + "\"                \n                                    data-bucket=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"bucket") || (depth0 != null ? lookupProperty(depth0,"bucket") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"bucket","hash":{},"data":data,"loc":{"start":{"line":626,"column":49},"end":{"line":626,"column":59}}}) : helper)))
    + "\"\n                                    data-path=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"fileName") || (depth0 != null ? lookupProperty(depth0,"fileName") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"fileName","hash":{},"data":data,"loc":{"start":{"line":627,"column":47},"end":{"line":627,"column":59}}}) : helper)))
    + "\"\n                                    onclick=\"download_from_bucket(event)\"    \n                                    title=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"fileName") || (depth0 != null ? lookupProperty(depth0,"fileName") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"fileName","hash":{},"data":data,"loc":{"start":{"line":629,"column":43},"end":{"line":629,"column":55}}}) : helper)))
    + "\"                           \n                                    href=\"#\"                                    \n                                >\n                                    "
    + alias4(((helper = (helper = lookupProperty(helpers,"fileName") || (depth0 != null ? lookupProperty(depth0,"fileName") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"fileName","hash":{},"data":data,"loc":{"start":{"line":632,"column":36},"end":{"line":632,"column":48}}}) : helper)))
    + "\n                                    \n                                </a>\n                            </div>  \n                            <div class=\"ms-2\">\n                                <i class=\"fs-1 fas fa-download text-primary\"></i>\n                            </div>                                 \n                        </li>\n\n                        <li class=\"list-group-item d-flex flex-row justify-content-between align-items-center\">\n                            <div class=\"pe-1\"><strong>Haystack tags:</strong></div>\n                            <div class=\"ps-1 pe-1 flex-shrink-1\">\n                                <div class=\"badge badge-secondary rounded-0 property-info-item\" title=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"haystack") || (depth0 != null ? lookupProperty(depth0,"haystack") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"haystack","hash":{},"data":data,"loc":{"start":{"line":644,"column":103},"end":{"line":644,"column":115}}}) : helper)))
    + "\">"
    + alias4(((helper = (helper = lookupProperty(helpers,"haystack") || (depth0 != null ? lookupProperty(depth0,"haystack") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"haystack","hash":{},"data":data,"loc":{"start":{"line":644,"column":117},"end":{"line":644,"column":129}}}) : helper)))
    + "</div>\n                            </div>  \n                        </li>                                               \n                    </ul>                    \n                </section>\n                <div class=\"vr vr-blurry align-self-stretch\"></div>\n                <section class=\"d-flex flex-column flex-shrink-1 property-section\">\n                    <h6 class=\"bg-light p-2 border-top border-bottom text-center\">\n                        Property Assigned Meters\n                        <span class=\"badge badge-secondary \">"
    + alias4(container.lambda(((stack1 = (depth0 != null ? lookupProperty(depth0,"weights") : depth0)) != null ? lookupProperty(stack1,"length") : stack1), depth0))
    + "</span>\n                    </h6>\n"
    + ((stack1 = lookupProperty(helpers,"if").call(alias1,((stack1 = (depth0 != null ? lookupProperty(depth0,"weights") : depth0)) != null ? lookupProperty(stack1,"length") : stack1),{"name":"if","hash":{},"fn":container.program(62, data, 0),"inverse":container.program(65, data, 0),"data":data,"loc":{"start":{"line":655,"column":20},"end":{"line":706,"column":27}}})) != null ? stack1 : "")
    + "                </section>\n                <div class=\"vr vr-blurry align-self-stretch\"></div>\n                <section class=\"d-flex flex-column flex-grow-1 property-section\">\n                    <h6 class=\"bg-light p-2 border-top border-bottom text-center\">\n                        Property Meter Charts\n                    </h6>\n                    <ul class=\"list-group list-group-light mb-2 h-100\">\n                        <li class=\"list-group-item d-flex flex-row justify-content-stretch\">\n                            <div class=\"note note-secondary flex-grow-1 p-1\">\n                                <p class=\"mb-0 text-center\">\n                                    Property Data in the Data Warehouse, last 72 hours\n                                </p>\n                            </div>\n                        </li>\n                        <li class=\"list-group-item d-flex flex-row justify-content-stretch h-100\">\n                             <div \n                                class=\"property_charts flex-grow-1 align-self-stretch\"\n                                id=\"chart_"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":724,"column":42},"end":{"line":724,"column":49}}}) : helper)))
    + "\" \n                            >\n                            </div>\n                        </li>\n                    </ul>                    \n                </section>\n            </div>\n        </div>  \n    </div>\n";
},"62":function(container,depth0,helpers,partials,data) {
    var stack1, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "                        <ul class=\"list-group list-group-light mb-2 overflow-auto\">\n"
    + ((stack1 = lookupProperty(helpers,"each").call(depth0 != null ? depth0 : (container.nullContext || {}),(depth0 != null ? lookupProperty(depth0,"weights") : depth0),{"name":"each","hash":{},"fn":container.program(63, data, 0),"inverse":container.noop,"data":data,"loc":{"start":{"line":657,"column":28},"end":{"line":694,"column":37}}})) != null ? stack1 : "")
    + "                        </ul>\n";
},"63":function(container,depth0,helpers,partials,data) {
    var helper, alias1=depth0 != null ? depth0 : (container.nullContext || {}), alias2=container.hooks.helperMissing, alias3="function", alias4=container.escapeExpression, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "                                <li class=\"list-group-item d-flex flex-row justify-content-between align-items-center\">\n                                    <div class=\"ms-1 me-1 d-flex align-items-center h-100\">\n                                        <iconify-icon icon=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"icon_type") || (depth0 != null ? lookupProperty(depth0,"icon_type") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"icon_type","hash":{},"data":data,"loc":{"start":{"line":660,"column":60},"end":{"line":660,"column":73}}}) : helper)))
    + "\" class =\"fs-1\"></iconify-icon>\n                                    </div>\n                                    <div class=\"d-flex flex-column flex-grow-1\">\n                                        <div class=\"flex-shrink-1 fs-1 ms-1 me-1\">\n                                            <div class=\"d-flex flex-row me-1 mb-1 \">\n                                                <div class=\"badge badge-secondary badge-property-header-meter rounded-0\">\n                                                    Meter:\n                                                </div>   \n                                                <div \n                                                    class=\"badge badge-primary flex-grow-1 fw-normal text-nowrap text truncate badge-property-meter rounded-0\"\n                                                    title=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"meter_uri") || (depth0 != null ? lookupProperty(depth0,"meter_uri") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"meter_uri","hash":{},"data":data,"loc":{"start":{"line":670,"column":59},"end":{"line":670,"column":72}}}) : helper)))
    + "\"\n                                                >\n                                                    "
    + alias4(((helper = (helper = lookupProperty(helpers,"meter_uri") || (depth0 != null ? lookupProperty(depth0,"meter_uri") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"meter_uri","hash":{},"data":data,"loc":{"start":{"line":672,"column":52},"end":{"line":672,"column":65}}}) : helper)))
    + "\n                                                </div>   \n                                            </div>                                            \n                                        </div>\n                                        <div class=\"d-flex flex-row justify-content-between align-items-center ms-1 me-2\">\n                                            <div class=\"d-flex flex-row me-1\">\n                                                <div class=\"badge badge-secondary rounded-0\">Type:</div>   \n                                                <div \n                                                    class=\"badge badge-info fw-normal text-nowrap text-truncate rounded-0\"\n                                                    title=\""
    + alias4(((helper = (helper = lookupProperty(helpers,"type") || (depth0 != null ? lookupProperty(depth0,"type") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"type","hash":{},"data":data,"loc":{"start":{"line":681,"column":59},"end":{"line":681,"column":67}}}) : helper)))
    + "\"\n                                                >\n                                                    "
    + alias4(((helper = (helper = lookupProperty(helpers,"type") || (depth0 != null ? lookupProperty(depth0,"type") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"type","hash":{},"data":data,"loc":{"start":{"line":683,"column":52},"end":{"line":683,"column":60}}}) : helper)))
    + "\n                                                </div>   \n                                            </div>\n                                            <div class=\"d-flex flex-row\">\n                                                <div class=\"badge badge-secondary rounded-0\">Weight:</div>   \n                                                <div class=\"badge badge-info rounded-0\">"
    + alias4(((helper = (helper = lookupProperty(helpers,"weight") || (depth0 != null ? lookupProperty(depth0,"weight") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"weight","hash":{},"data":data,"loc":{"start":{"line":688,"column":88},"end":{"line":688,"column":98}}}) : helper)))
    + "</div>   \n                                            </div>\n                                        </div>\n\n                                    </div>\n                                </li>\n";
},"65":function(container,depth0,helpers,partials,data) {
    return "                        <ul class=\"list-group list-group-light mb-2 overflow-auto\">\n                             <li class=\"list-group-item d-flex flex-row justify-content-stretch\">\n                                <div class=\"note note-danger flex-grow-1 p-1\">\n                                    <p class=\"mb-0\">\n                                       Property does not have assigned meters\n                                    </p>\n                                </div>\n                             </li>\n                        </ul>\n";
},"67":function(container,depth0,helpers,partials,data) {
    var stack1, helper, alias1=depth0 != null ? depth0 : (container.nullContext || {}), alias2=container.hooks.helperMissing, alias3="function", alias4=container.escapeExpression, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "    <div class=\"card border border-secondary shadow-0 rounded-0 mb-1\">\n        <div class=\"card-header text-white bg-primary pt-0 pb-0 rounded-0\">\n            <div class=\"d-flex flex-row justify-content-between\">\n                <div class=\"d-flex flex-row justify-content-start align-items-center\">\n                    <div class=\"ps-1 pe-1 jbb-participant-name-conteiner\">\n                        <h1 class=\"m-0\">\n                            <strong>"
    + alias4(((helper = (helper = lookupProperty(helpers,"name") || (depth0 != null ? lookupProperty(depth0,"name") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"name","hash":{},"data":data,"loc":{"start":{"line":743,"column":36},"end":{"line":743,"column":44}}}) : helper)))
    + "</strong> \n                        </h1>\n                    </div>   \n"
    + ((stack1 = container.invokePartial(lookupProperty(partials,"Divider"),depth0,{"name":"Divider","data":data,"indent":"                    ","helpers":helpers,"partials":partials,"decorators":container.decorators})) != null ? stack1 : "")
    + "                    <div class=\"ps-1 pe-1\">\n                        <span class=\"badge rounded-pill badge-"
    + ((stack1 = lookupProperty(helpers,"if").call(alias1,(depth0 != null ? lookupProperty(depth0,"admin") : depth0),{"name":"if","hash":{},"fn":container.program(68, data, 0),"inverse":container.program(70, data, 0),"data":data,"loc":{"start":{"line":748,"column":62},"end":{"line":748,"column":106}}})) != null ? stack1 : "")
    + "\">\n                            "
    + ((stack1 = lookupProperty(helpers,"if").call(alias1,(depth0 != null ? lookupProperty(depth0,"admin") : depth0),{"name":"if","hash":{},"fn":container.program(72, data, 0),"inverse":container.program(74, data, 0),"data":data,"loc":{"start":{"line":749,"column":28},"end":{"line":749,"column":73}}})) != null ? stack1 : "")
    + "\n                        </span>\n                    </div>\n"
    + ((stack1 = container.invokePartial(lookupProperty(partials,"Divider"),depth0,{"name":"Divider","data":data,"indent":"                    ","helpers":helpers,"partials":partials,"decorators":container.decorators})) != null ? stack1 : "")
    + "                    <div class=\"ps-1 pe-1\">\n                        <a\n                            class=\"btn btn-tag btn-rounded btn-outline-secondary text-white\"\n                            href=\"https://console.cloud.google.com/storage/browser/"
    + alias4(((helper = (helper = lookupProperty(helpers,"bucket") || (depth0 != null ? lookupProperty(depth0,"bucket") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"bucket","hash":{},"data":data,"loc":{"start":{"line":756,"column":83},"end":{"line":756,"column":93}}}) : helper)))
    + ";tab=objects?forceOnBucketsSortingFiltering=false&project="
    + alias4(((helper = (helper = lookupProperty(helpers,"env") || (depth0 != null ? lookupProperty(depth0,"env") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"env","hash":{},"data":data,"loc":{"start":{"line":756,"column":151},"end":{"line":756,"column":158}}}) : helper)))
    + "&prefix=&forceOnObjectsSortingFiltering=false\"\n                            target=\"_blank\"\n                            data-mdb-toggle=\"tooltip\" \n                            data-mdb-placement=\"right\"\n                            title=\"Participant's Google Storage Bucket\"\n                        >\n                            <i class=\"fa-sharp fa-solid fa-hard-drive\"></i>\n                            "
    + alias4(((helper = (helper = lookupProperty(helpers,"bucket") || (depth0 != null ? lookupProperty(depth0,"bucket") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"bucket","hash":{},"data":data,"loc":{"start":{"line":763,"column":28},"end":{"line":763,"column":38}}}) : helper)))
    + "\n                        </a>\n                    </div>                                        \n                </div>\n\n                <div class=\"d-flex flex-row justify-content-start align-items-center\">\n                    <div class=\"ps-1 pe-1\"> "
    + ((stack1 = container.invokePartial(lookupProperty(partials,"ParticipantDashboardMenu"),depth0,{"name":"ParticipantDashboardMenu","data":data,"helpers":helpers,"partials":partials,"decorators":container.decorators})) != null ? stack1 : "")
    + " </div>    \n                    <div class=\"ps-1 pe-5\"> "
    + ((stack1 = container.invokePartial(lookupProperty(partials,"AccessCodesMenu"),depth0,{"name":"AccessCodesMenu","data":data,"helpers":helpers,"partials":partials,"decorators":container.decorators})) != null ? stack1 : "")
    + " </div>\n"
    + ((stack1 = container.invokePartial(lookupProperty(partials,"CardToggler"),depth0,{"name":"CardToggler","hash":{"color":"white"},"data":data,"indent":"                    ","helpers":helpers,"partials":partials,"decorators":container.decorators})) != null ? stack1 : "")
    + "                </div>\n            </div>        \n        </div>\n        <div class=\"card-body collapse show\" id=\"collapse_"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":775,"column":58},"end":{"line":775,"column":65}}}) : helper)))
    + "\">\n            <div class=\"accordion\" id=\"participant_accordion_"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":776,"column":61},"end":{"line":776,"column":68}}}) : helper)))
    + "\">\n                <div class=\"accordion-item participan-meters-accordion-item\">\n                    <h2 class=\"accordion-header\" id=\"flush-heading-meters-"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":778,"column":74},"end":{"line":778,"column":81}}}) : helper)))
    + "\">\n                        <button \n                            class=\"accordion-button p-2\" \n                            type=\"button\" \n                            data-mdb-toggle=\"collapse\"\n                            data-mdb-target=\"#flush-collapse-meters-"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":783,"column":68},"end":{"line":783,"column":75}}}) : helper)))
    + "\" \n                            aria-expanded=\"true\" \n                            aria-controls=\"flush-collapse-meters-"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":785,"column":65},"end":{"line":785,"column":72}}}) : helper)))
    + "\"\n                        >\n                            <i class=\"fs-2 bi bi-speedometer me-2 opacity-70\"></i>\n                            Participant Meters Data\n                        </button>\n                    </h2>\n                    <div \n                        id=\"flush-collapse-meters-"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":792,"column":50},"end":{"line":792,"column":57}}}) : helper)))
    + "\" \n                        class=\"accordion-collapse collapse show\" \n                        aria-labelledby=\"flush-heading-meters-"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":794,"column":62},"end":{"line":794,"column":69}}}) : helper)))
    + "\" \n                    >\n                        <div class=\"accordion-body overflow-auto\">\n"
    + ((stack1 = lookupProperty(helpers,"each").call(alias1,(depth0 != null ? lookupProperty(depth0,"meters") : depth0),{"name":"each","hash":{},"fn":container.program(76, data, 0),"inverse":container.noop,"data":data,"loc":{"start":{"line":797,"column":28},"end":{"line":799,"column":37}}})) != null ? stack1 : "")
    + "                        </div>\n                    </div>\n                </div>\n"
    + ((stack1 = lookupProperty(helpers,"if").call(alias1,((stack1 = (depth0 != null ? lookupProperty(depth0,"pushes") : depth0)) != null ? lookupProperty(stack1,"length") : stack1),{"name":"if","hash":{},"fn":container.program(78, data, 0),"inverse":container.noop,"data":data,"loc":{"start":{"line":803,"column":16},"end":{"line":832,"column":23}}})) != null ? stack1 : "")
    + "                <div class=\"accordion-item\">\n                    <h2 class=\"accordion-header\" id=\"flush-properties-heading-"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":834,"column":78},"end":{"line":834,"column":85}}}) : helper)))
    + "\">\n                        <button \n                            class=\"accordion-button p-2\" \n                            type=\"button\" \n                            data-mdb-toggle=\"collapse\"\n                            data-mdb-target=\"#flush-collapse-properties-heading-"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":839,"column":80},"end":{"line":839,"column":87}}}) : helper)))
    + "\" \n                            aria-expanded=\"true\" \n                            aria-controls=\"flush-collapse-properties-"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":841,"column":69},"end":{"line":841,"column":76}}}) : helper)))
    + "\"\n                        >\n                            <i class=\"fs-2 fas fa-city me-2 opacity-70\"></i>\n                            Properties\n                        </button>\n                    </h2>     \n                        <div \n                            id=\"flush-collapse-properties-heading-"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":848,"column":66},"end":{"line":848,"column":73}}}) : helper)))
    + "\" \n                            class=\"accordion-collapse collapse\" \n                            aria-labelledby=\"flush-properties-heading-"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":850,"column":70},"end":{"line":850,"column":77}}}) : helper)))
    + "\" \n                        >\n                            <div class=\"accordion-body\">\n"
    + ((stack1 = lookupProperty(helpers,"if").call(alias1,((stack1 = (depth0 != null ? lookupProperty(depth0,"properties") : depth0)) != null ? lookupProperty(stack1,"length") : stack1),{"name":"if","hash":{},"fn":container.program(81, data, 0),"inverse":container.noop,"data":data,"loc":{"start":{"line":854,"column":32},"end":{"line":858,"column":39}}})) != null ? stack1 : "")
    + "                            </div>\n                        </div>                                   \n                </div>                \n            </div>\n\n\n\n\n\n\n        \n        </div>\n    </div>\n";
},"68":function(container,depth0,helpers,partials,data) {
    return "success";
},"70":function(container,depth0,helpers,partials,data) {
    return "secondary";
},"72":function(container,depth0,helpers,partials,data) {
    return " Admin ";
},"74":function(container,depth0,helpers,partials,data) {
    return " Operator ";
},"76":function(container,depth0,helpers,partials,data) {
    var stack1, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return ((stack1 = container.invokePartial(lookupProperty(partials,"MeterCard"),depth0,{"name":"MeterCard","data":data,"indent":"                                ","helpers":helpers,"partials":partials,"decorators":container.decorators})) != null ? stack1 : "");
},"78":function(container,depth0,helpers,partials,data) {
    var stack1, helper, alias1=depth0 != null ? depth0 : (container.nullContext || {}), alias2=container.hooks.helperMissing, alias3="function", alias4=container.escapeExpression, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "                    <div class=\"accordion-item\">\n                        <h2 class=\"accordion-header\" id=\"flush-meters-uploads-heading-"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":805,"column":86},"end":{"line":805,"column":93}}}) : helper)))
    + "\">\n                            <button \n                                class=\"accordion-button p-2\" \n                                type=\"button\" \n                                data-mdb-toggle=\"collapse\"\n                                data-mdb-target=\"#flush-collapse-meters-uploads-"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":810,"column":80},"end":{"line":810,"column":87}}}) : helper)))
    + "\" \n                                aria-expanded=\"true\" \n                                aria-controls=\"flush-collapse-meters-uploads-"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":812,"column":77},"end":{"line":812,"column":84}}}) : helper)))
    + "\"\n                            >\n                                <i class=\"fs-2 fas fa-upload me-2 opacity-70\"></i>\n                                Meters Manual Data Uploads\n                            </button>\n                        </h2>\n                        <div \n                            id=\"flush-collapse-meters-uploads-"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":819,"column":62},"end":{"line":819,"column":69}}}) : helper)))
    + "\" \n                            class=\"accordion-collapse collapse\" \n                            aria-labelledby=\"flush-meters-uploads-heading-"
    + alias4(((helper = (helper = lookupProperty(helpers,"idx") || (depth0 != null ? lookupProperty(depth0,"idx") : depth0)) != null ? helper : alias2),(typeof helper === alias3 ? helper.call(alias1,{"name":"idx","hash":{},"data":data,"loc":{"start":{"line":821,"column":74},"end":{"line":821,"column":81}}}) : helper)))
    + "\" \n                        >\n                            <div class=\"accordion-body\">\n"
    + ((stack1 = lookupProperty(helpers,"each").call(alias1,(depth0 != null ? lookupProperty(depth0,"pushes") : depth0),{"name":"each","hash":{},"fn":container.program(79, data, 0),"inverse":container.noop,"data":data,"loc":{"start":{"line":825,"column":32},"end":{"line":827,"column":41}}})) != null ? stack1 : "")
    + "                            </div>\n                        </div>\n                    </div>\n\n";
},"79":function(container,depth0,helpers,partials,data) {
    var stack1, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return ((stack1 = container.invokePartial(lookupProperty(partials,"PushesCard"),depth0,{"name":"PushesCard","data":data,"indent":"                                    ","helpers":helpers,"partials":partials,"decorators":container.decorators})) != null ? stack1 : "");
},"81":function(container,depth0,helpers,partials,data) {
    var stack1, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return ((stack1 = lookupProperty(helpers,"each").call(depth0 != null ? depth0 : (container.nullContext || {}),(depth0 != null ? lookupProperty(depth0,"properties") : depth0),{"name":"each","hash":{},"fn":container.program(82, data, 0),"inverse":container.noop,"data":data,"loc":{"start":{"line":855,"column":36},"end":{"line":857,"column":45}}})) != null ? stack1 : "");
},"82":function(container,depth0,helpers,partials,data) {
    var stack1, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return ((stack1 = container.invokePartial(lookupProperty(partials,"PropertiesCard"),depth0,{"name":"PropertiesCard","data":data,"indent":"                                        ","helpers":helpers,"partials":partials,"decorators":container.decorators})) != null ? stack1 : "");
},"compiler":[8,">= 4.3.0"],"main":function(container,depth0,helpers,partials,data,blockParams,depths) {
    var stack1, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  return "\n\n\n\n\n\n<div class=\"d-flex flex-row flex-grow-1 align-items-center justify-content-center mb-1\">\n    <div class=\"flex-shrink-1\">\n        <div class=\"note note-primary pt-1 pb-1\">\n            <p class=\"mb-0\">\n                You have access to the following participants, given your membership in the corresponding participant-specific user groups\n            </p>\n        </div>\n    </div>\n    <div class=\"flex-fill\"></div>\n    <div class=\"flex-shrink-1\">\n        <div class=\"note note-primary pt-1 pb-1\">\n            <p class=\"mb-0\">\n                Updated from Google Data Warehouse <strong>Hourly</strong>        \n            </p>\n        </div>        \n    </div>\n</div>\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n"
    + ((stack1 = lookupProperty(helpers,"each").call(depth0 != null ? depth0 : (container.nullContext || {}),(depth0 != null ? lookupProperty(depth0,"participants") : depth0),{"name":"each","hash":{},"fn":container.program(67, data, 0, blockParams, depths),"inverse":container.noop,"data":data,"loc":{"start":{"line":736,"column":0},"end":{"line":873,"column":9}}})) != null ? stack1 : "")
    + "\n";
},"main_d":  function(fn, props, container, depth0, data, blockParams, depths) {

  var decorators = container.decorators, lookupProperty = container.lookupProperty || function(parent, propertyName) {
        if (Object.prototype.hasOwnProperty.call(parent, propertyName)) {
          return parent[propertyName];
        }
        return undefined
    };

  fn = lookupProperty(decorators,"inline")(fn,props,container,{"name":"inline","hash":{},"fn":container.program(1, data, 0, blockParams, depths),"inverse":container.noop,"args":["downloadPartial"],"data":data,"loc":{"start":{"line":1,"column":0},"end":{"line":16,"column":11}}}) || fn;
  fn = lookupProperty(decorators,"inline")(fn,props,container,{"name":"inline","hash":{},"fn":container.program(3, data, 0, blockParams, depths),"inverse":container.noop,"args":["uploadPartial"],"data":data,"loc":{"start":{"line":18,"column":0},"end":{"line":29,"column":11}}}) || fn;
  fn = lookupProperty(decorators,"inline")(fn,props,container,{"name":"inline","hash":{},"fn":container.program(5, data, 0, blockParams, depths),"inverse":container.noop,"args":["linkNewPage"],"data":data,"loc":{"start":{"line":32,"column":0},"end":{"line":41,"column":11}}}) || fn;
  fn = lookupProperty(decorators,"inline")(fn,props,container,{"name":"inline","hash":{},"fn":container.program(7, data, 0, blockParams, depths),"inverse":container.noop,"args":["Divider"],"data":data,"loc":{"start":{"line":44,"column":0},"end":{"line":48,"column":11}}}) || fn;
  fn = lookupProperty(decorators,"inline")(fn,props,container,{"name":"inline","hash":{},"fn":container.program(9, data, 0, blockParams, depths),"inverse":container.noop,"args":["DownloadFile"],"data":data,"loc":{"start":{"line":69,"column":0},"end":{"line":91,"column":11}}}) || fn;
  fn = lookupProperty(decorators,"inline")(fn,props,container,{"name":"inline","hash":{},"fn":container.program(11, data, 0, blockParams, depths),"inverse":container.noop,"args":["DownloadDropDownNode"],"data":data,"loc":{"start":{"line":94,"column":0},"end":{"line":115,"column":11}}}) || fn;
  fn = lookupProperty(decorators,"inline")(fn,props,container,{"name":"inline","hash":{},"fn":container.program(13, data, 0, blockParams, depths),"inverse":container.noop,"args":["ParticipantDashboardMenu"],"data":data,"loc":{"start":{"line":118,"column":0},"end":{"line":207,"column":11}}}) || fn;
  fn = lookupProperty(decorators,"inline")(fn,props,container,{"name":"inline","hash":{},"fn":container.program(23, data, 0, blockParams, depths),"inverse":container.noop,"args":["AccessCodesMenu"],"data":data,"loc":{"start":{"line":210,"column":0},"end":{"line":266,"column":11}}}) || fn;
  fn = lookupProperty(decorators,"inline")(fn,props,container,{"name":"inline","hash":{},"fn":container.program(33, data, 0, blockParams, depths),"inverse":container.noop,"args":["CardToggler"],"data":data,"loc":{"start":{"line":269,"column":0},"end":{"line":280,"column":11}}}) || fn;
  fn = lookupProperty(decorators,"inline")(fn,props,container,{"name":"inline","hash":{},"fn":container.program(35, data, 0, blockParams, depths),"inverse":container.noop,"args":["FilesNotLoaaded"],"data":data,"loc":{"start":{"line":282,"column":0},"end":{"line":296,"column":11}}}) || fn;
  fn = lookupProperty(decorators,"inline")(fn,props,container,{"name":"inline","hash":{},"fn":container.program(37, data, 0, blockParams, depths),"inverse":container.noop,"args":["MeterCard"],"data":data,"loc":{"start":{"line":299,"column":0},"end":{"line":434,"column":11}}}) || fn;
  fn = lookupProperty(decorators,"inline")(fn,props,container,{"name":"inline","hash":{},"fn":container.program(55, data, 0, blockParams, depths),"inverse":container.noop,"args":["PushesCard"],"data":data,"loc":{"start":{"line":437,"column":0},"end":{"line":548,"column":11}}}) || fn;
  fn = lookupProperty(decorators,"inline")(fn,props,container,{"name":"inline","hash":{},"fn":container.program(61, data, 0, blockParams, depths),"inverse":container.noop,"args":["PropertiesCard"],"data":data,"loc":{"start":{"line":551,"column":0},"end":{"line":733,"column":11}}}) || fn;
  return fn;
  }

,"useDecorators":true,"usePartial":true,"useData":true,"useDepths":true});
})();