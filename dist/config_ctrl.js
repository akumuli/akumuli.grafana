///<reference path="../../../headers/common.d.ts" />
System.register([], function (exports_1, context_1) {
    "use strict";
    var __moduleName = context_1 && context_1.id;
    var AkumuliConfigCtrl;
    return {
        setters: [],
        execute: function () {///<reference path="../../../headers/common.d.ts" />
            AkumuliConfigCtrl = (function () {
                /** @ngInject */
                function AkumuliConfigCtrl($scope) {
                    this.tsdbVersions = [
                        { name: '==0.7', value: 1 },
                    ];
                    this.current.jsonData = this.current.jsonData || {};
                    this.current.jsonData.tsdbVersion = this.current.jsonData.tsdbVersion || 1;
                }
                return AkumuliConfigCtrl;
            }());
            AkumuliConfigCtrl.templateUrl = 'public/app/plugins/datasource/akumuli-datasource/partials/config.html';
            exports_1("AkumuliConfigCtrl", AkumuliConfigCtrl);
        }
    };
});
//# sourceMappingURL=config_ctrl.js.map