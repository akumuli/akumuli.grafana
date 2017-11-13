System.register(["./datasource", "./query_ctrl", "./config_ctrl"], function (exports_1, context_1) {
    "use strict";
    var __moduleName = context_1 && context_1.id;
    var datasource_1, query_ctrl_1, config_ctrl_1, AnnotationsQueryCtrl;
    return {
        setters: [
            function (datasource_1_1) {
                datasource_1 = datasource_1_1;
            },
            function (query_ctrl_1_1) {
                query_ctrl_1 = query_ctrl_1_1;
            },
            function (config_ctrl_1_1) {
                config_ctrl_1 = config_ctrl_1_1;
            }
        ],
        execute: function () {
            exports_1("Datasource", datasource_1.AkumuliDatasource);
            exports_1("QueryCtrl", query_ctrl_1.AkumuliQueryCtrl);
            exports_1("ConfigCtrl", config_ctrl_1.AkumuliConfigCtrl);
            AnnotationsQueryCtrl = (function () {
                function AnnotationsQueryCtrl() {
                }
                return AnnotationsQueryCtrl;
            }());
            AnnotationsQueryCtrl.templateUrl = 'partials/annotations.editor.html';
            exports_1("AnnotationsQueryCtrl", AnnotationsQueryCtrl);
        }
    };
});
//# sourceMappingURL=module.js.map