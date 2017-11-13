System.register(["test/lib/common", "test/specs/helpers", "../query_ctrl"], function (exports_1, context_1) {
    "use strict";
    var __moduleName = context_1 && context_1.id;
    var common_1, helpers_1, query_ctrl_1;
    return {
        setters: [
            function (common_1_1) {
                common_1 = common_1_1;
            },
            function (helpers_1_1) {
                helpers_1 = helpers_1_1;
            },
            function (query_ctrl_1_1) {
                query_ctrl_1 = query_ctrl_1_1;
            }
        ],
        execute: function () {
            common_1.describe('AkumuliQueryCtrl', function () {
                var ctx = new helpers_1.default.ControllerTestContext();
                common_1.beforeEach(common_1.angularMocks.module('grafana.core'));
                common_1.beforeEach(common_1.angularMocks.module('grafana.services'));
                common_1.beforeEach(common_1.angularMocks.module(function ($compileProvider) {
                    $compileProvider.preAssignBindingsEnabled(true);
                }));
                common_1.beforeEach(ctx.providePhase(['backendSrv', 'templateSrv']));
                common_1.beforeEach(ctx.providePhase());
                common_1.beforeEach(common_1.angularMocks.inject(function ($rootScope, $controller, $q) {
                    ctx.$q = $q;
                    ctx.scope = $rootScope.$new();
                    ctx.target = { target: '' };
                    ctx.panelCtrl = { panel: {} };
                    ctx.panelCtrl.refresh = common_1.sinon.spy();
                    ctx.datasource.getAggregators = common_1.sinon.stub().returns(ctx.$q.when([]));
                    ctx.datasource.getFilterTypes = common_1.sinon.stub().returns(ctx.$q.when([]));
                    ctx.ctrl = $controller(query_ctrl_1.AkumuliQueryCtrl, { $scope: ctx.scope }, {
                        panelCtrl: ctx.panelCtrl,
                        datasource: ctx.datasource,
                        target: ctx.target,
                    });
                    ctx.scope.$digest();
                }));
                common_1.describe('init query_ctrl variables', function () {
                    common_1.it('filter types should be initialized', function () {
                        common_1.expect(ctx.ctrl.filterTypes.length).to.be(7);
                    });
                    common_1.it('aggregators should be initialized', function () {
                        common_1.expect(ctx.ctrl.aggregators.length).to.be(8);
                    });
                    common_1.it('fill policy options should be initialized', function () {
                        common_1.expect(ctx.ctrl.fillPolicies.length).to.be(4);
                    });
                });
                common_1.describe('when adding filters and tags', function () {
                    common_1.it('addTagMode should be false when closed', function () {
                        ctx.ctrl.addTagMode = true;
                        ctx.ctrl.closeAddTagMode();
                        common_1.expect(ctx.ctrl.addTagMode).to.be(false);
                    });
                    common_1.it('addFilterMode should be false when closed', function () {
                        ctx.ctrl.addFilterMode = true;
                        ctx.ctrl.closeAddFilterMode();
                        common_1.expect(ctx.ctrl.addFilterMode).to.be(false);
                    });
                    common_1.it('removing a tag from the tags list', function () {
                        ctx.ctrl.target.tags = { "tagk": "tag_key", "tagk2": "tag_value2" };
                        ctx.ctrl.removeTag("tagk");
                        common_1.expect(Object.keys(ctx.ctrl.target.tags).length).to.be(1);
                    });
                    common_1.it('removing a filter from the filters list', function () {
                        ctx.ctrl.target.filters = [{ "tagk": "tag_key", "filter": "tag_value2", "type": "wildcard", "groupBy": true }];
                        ctx.ctrl.removeFilter(0);
                        common_1.expect(ctx.ctrl.target.filters.length).to.be(0);
                    });
                    common_1.it('adding a filter when tags exist should generate error', function () {
                        ctx.ctrl.target.tags = { "tagk": "tag_key", "tagk2": "tag_value2" };
                        ctx.ctrl.addFilter();
                        common_1.expect(ctx.ctrl.errors.filters).to.be('Please remove tags to use filters, tags and filters are mutually exclusive.');
                    });
                    common_1.it('adding a tag when filters exist should generate error', function () {
                        ctx.ctrl.target.filters = [{ "tagk": "tag_key", "filter": "tag_value2", "type": "wildcard", "groupBy": true }];
                        ctx.ctrl.addTag();
                        common_1.expect(ctx.ctrl.errors.tags).to.be('Please remove filters to use tags, tags and filters are mutually exclusive.');
                    });
                });
            });
        }
    };
});
//# sourceMappingURL=query-ctrl-specs.js.map