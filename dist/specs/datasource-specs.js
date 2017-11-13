System.register(["test/lib/common", "test/specs/helpers", "../datasource"], function (exports_1, context_1) {
    "use strict";
    var __moduleName = context_1 && context_1.id;
    var common_1, helpers_1, datasource_1;
    return {
        setters: [
            function (common_1_1) {
                common_1 = common_1_1;
            },
            function (helpers_1_1) {
                helpers_1 = helpers_1_1;
            },
            function (datasource_1_1) {
                datasource_1 = datasource_1_1;
            }
        ],
        execute: function () {
            common_1.describe('opentsdb', function () {
                var ctx = new helpers_1.default.ServiceTestContext();
                var instanceSettings = { url: '', jsonData: { tsdbVersion: 1 } };
                common_1.beforeEach(common_1.angularMocks.module('grafana.core'));
                common_1.beforeEach(common_1.angularMocks.module('grafana.services'));
                common_1.beforeEach(ctx.providePhase(['backendSrv']));
                common_1.beforeEach(common_1.angularMocks.inject(function ($q, $rootScope, $httpBackend, $injector) {
                    ctx.$q = $q;
                    ctx.$httpBackend = $httpBackend;
                    ctx.$rootScope = $rootScope;
                    ctx.ds = $injector.instantiate(datasource_1.AkumuliDatasource, { instanceSettings: instanceSettings });
                    $httpBackend.when('GET', /\.html$/).respond('');
                }));
                common_1.describe('When performing metricFindQuery', function () {
                    var results;
                    var requestOptions;
                    common_1.beforeEach(function () {
                        ctx.backendSrv.datasourceRequest = function (options) {
                            requestOptions = options;
                            return ctx.$q.when({ data: [{ target: 'prod1.count', datapoints: [[10, 1], [12, 1]] }] });
                        };
                    });
                    common_1.it('metrics() should generate api suggest query', function () {
                        ctx.ds.metricFindQuery('metrics(pew)').then(function (data) { results = data; });
                        ctx.$rootScope.$apply();
                        common_1.expect(requestOptions.url).to.be('/api/suggest');
                        common_1.expect(requestOptions.params.type).to.be('metrics');
                        common_1.expect(requestOptions.params.q).to.be('pew');
                    });
                    common_1.it('tag_names(cpu) should generate lookup query', function () {
                        ctx.ds.metricFindQuery('tag_names(cpu)').then(function (data) { results = data; });
                        ctx.$rootScope.$apply();
                        common_1.expect(requestOptions.url).to.be('/api/search/lookup');
                        common_1.expect(requestOptions.params.m).to.be('cpu');
                    });
                    common_1.it('tag_values(cpu, test) should generate lookup query', function () {
                        ctx.ds.metricFindQuery('tag_values(cpu, hostname)').then(function (data) { results = data; });
                        ctx.$rootScope.$apply();
                        common_1.expect(requestOptions.url).to.be('/api/search/lookup');
                        common_1.expect(requestOptions.params.m).to.be('cpu{hostname=*}');
                    });
                    common_1.it('tag_values(cpu, test) should generate lookup query', function () {
                        ctx.ds.metricFindQuery('tag_values(cpu, hostname, env=$env)').then(function (data) { results = data; });
                        ctx.$rootScope.$apply();
                        common_1.expect(requestOptions.url).to.be('/api/search/lookup');
                        common_1.expect(requestOptions.params.m).to.be('cpu{hostname=*,env=$env}');
                    });
                    common_1.it('tag_values(cpu, test) should generate lookup query', function () {
                        ctx.ds.metricFindQuery('tag_values(cpu, hostname, env=$env, region=$region)').then(function (data) { results = data; });
                        ctx.$rootScope.$apply();
                        common_1.expect(requestOptions.url).to.be('/api/search/lookup');
                        common_1.expect(requestOptions.params.m).to.be('cpu{hostname=*,env=$env,region=$region}');
                    });
                    common_1.it('suggest_tagk() should generate api suggest query', function () {
                        ctx.ds.metricFindQuery('suggest_tagk(foo)').then(function (data) { results = data; });
                        ctx.$rootScope.$apply();
                        common_1.expect(requestOptions.url).to.be('/api/suggest');
                        common_1.expect(requestOptions.params.type).to.be('tagk');
                        common_1.expect(requestOptions.params.q).to.be('foo');
                    });
                    common_1.it('suggest_tagv() should generate api suggest query', function () {
                        ctx.ds.metricFindQuery('suggest_tagv(bar)').then(function (data) { results = data; });
                        ctx.$rootScope.$apply();
                        common_1.expect(requestOptions.url).to.be('/api/suggest');
                        common_1.expect(requestOptions.params.type).to.be('tagv');
                        common_1.expect(requestOptions.params.q).to.be('bar');
                    });
                });
            });
        }
    };
});
//# sourceMappingURL=datasource-specs.js.map