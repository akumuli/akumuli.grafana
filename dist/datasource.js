///<reference path="../../../headers/common.d.ts" />
System.register(["lodash", "moment"], function (exports_1, context_1) {
    "use strict";
    var __moduleName = context_1 && context_1.id;
    var lodash_1, moment_1, AkumuliDatasource;
    return {
        setters: [
            function (lodash_1_1) {
                lodash_1 = lodash_1_1;
            },
            function (moment_1_1) {
                moment_1 = moment_1_1;
            }
        ],
        execute: function () {///<reference path="../../../headers/common.d.ts" />
            AkumuliDatasource = (function () {
                /** @ngInject */
                function AkumuliDatasource(instanceSettings, backendSrv, $q) {
                    this.instanceSettings = instanceSettings;
                    this.backendSrv = backendSrv;
                    this.$q = $q;
                }
                /** Test that datasource connection works */
                AkumuliDatasource.prototype.testDatasource = function () {
                    var options = {
                        method: "GET",
                        url: this.instanceSettings.url + "/api/stats",
                        data: ""
                    };
                    return this.backendSrv.datasourceRequest(options).then(function (res) {
                        return { status: "success", message: "Data source is working", title: "Success" };
                    });
                };
                AkumuliDatasource.prototype.metricFindQuery = function (metricName) {
                    var requestBody = {
                        select: "metric-names",
                        "starts-with": metricName
                    };
                    var httpRequest = {
                        method: "POST",
                        url: this.instanceSettings.url + "/api/suggest",
                        data: requestBody
                    };
                    return this.backendSrv.datasourceRequest(httpRequest).then(function (res) {
                        var data = [];
                        if (res.status === 'error') {
                            throw res.error;
                        }
                        if (res.data.charAt(0) === '-') {
                            throw { message: res.data.substr(1) };
                        }
                        var lines = res.data.split("\r\n");
                        lodash_1.default.forEach(lines, function (line) {
                            if (line) {
                                var name = line.substr(1);
                                data.push({ text: name, value: name });
                            }
                        });
                        return data;
                    });
                };
                /** Parse series name in a canonical form */
                AkumuliDatasource.prototype.extractTags = function (names) {
                    var where = [];
                    lodash_1.default.forEach(names, function (name) {
                        var tags = name.split(' ');
                        if (tags.length < 2) {
                            // This shouldn't happen since series name should
                            // contain a metric name and at least one tag.
                            throw "bad metric name received";
                        }
                        var tagset = {};
                        for (var i = 1; i < tags.length; i++) {
                            var kv = tags[i].split('=');
                            var tag = kv[0];
                            var value = kv[1];
                            tagset[tag] = value;
                        }
                        where.push(tagset);
                    });
                    return where;
                };
                AkumuliDatasource.prototype.annotationQuery = function (options) {
                    return this.backendSrv.get('/api/annotations', {
                        from: options.range.from.valueOf(),
                        to: options.range.to.valueOf(),
                        limit: options.limit,
                        type: options.type,
                    });
                };
                AkumuliDatasource.prototype.getAggregators = function () {
                    // TODO: query aggregators from Akumuli
                    return new Promise(function (resolve, reject) {
                        resolve(["mean", "sum", "count", "min", "max"]);
                    });
                };
                AkumuliDatasource.prototype.suggestTagKeys = function (metric, tagPrefix) {
                    tagPrefix = tagPrefix || "";
                    var requestBody = {
                        select: "tag-names",
                        metric: metric,
                        "starts-with": tagPrefix
                    };
                    var httpRequest = {
                        method: "POST",
                        url: this.instanceSettings.url + "/api/suggest",
                        data: requestBody
                    };
                    return this.backendSrv.datasourceRequest(httpRequest).then(function (res) {
                        var data = [];
                        if (res.status === 'error') {
                            throw res.error;
                        }
                        if (res.data.charAt(0) === '-') {
                            throw { message: res.data.substr(1) };
                        }
                        var lines = res.data.split("\r\n");
                        lodash_1.default.forEach(lines, function (line) {
                            if (line) {
                                var name = line.substr(1);
                                data.push({ text: name, value: name });
                            }
                        });
                        return data;
                    });
                };
                AkumuliDatasource.prototype.suggestTagValues = function (metric, tagName, valuePrefix) {
                    tagName = tagName || "";
                    valuePrefix = valuePrefix || "";
                    var requestBody = {
                        select: "tag-values",
                        metric: metric,
                        tag: tagName,
                        "starts-with": valuePrefix
                    };
                    var httpRequest = {
                        method: "POST",
                        url: this.instanceSettings.url + "/api/suggest",
                        data: requestBody
                    };
                    return this.backendSrv.datasourceRequest(httpRequest).then(function (res) {
                        var data = [];
                        if (res.status === 'error') {
                            throw res.error;
                        }
                        if (res.data.charAt(0) === '-') {
                            throw { message: res.data.substr(1) };
                        }
                        var lines = res.data.split("\r\n");
                        lodash_1.default.forEach(lines, function (line) {
                            if (line) {
                                var name = line.substr(1);
                                data.push({ text: name, value: name });
                            }
                        });
                        return data;
                    });
                };
                /** Query time-series storage */
                AkumuliDatasource.prototype.groupAggregateTopNQuery = function (begin, end, interval, limit, target) {
                    var _this = this;
                    // Use all the same parametres as original query
                    // but add 'top' function to the 'apply' clause.
                    // Extract tags from results and run 'select' query
                    // nomrally.
                    var metricName = target.metric;
                    var tags = target.tags;
                    var isTop = target.topN ? true : false;
                    var topN = target.topN;
                    if (!isTop) {
                        throw "top-N parameter required";
                    }
                    var query = {
                        select: metricName,
                        range: {
                            from: begin.format('YYYYMMDDTHHmmss.SSS'),
                            to: end.format('YYYYMMDDTHHmmss.SSS')
                        },
                        where: tags,
                        "order-by": "series",
                        apply: [{ name: "top", N: topN }]
                    };
                    var httpRequest = {
                        method: "POST",
                        url: this.instanceSettings.url + "/api/query",
                        data: query
                    };
                    return this.backendSrv.datasourceRequest(httpRequest).then(function (res) {
                        var data = [];
                        if (res.status === 'error') {
                            throw res.error;
                        }
                        if (res.data.charAt(0) === '-') {
                            throw { message: "Query error: " + res.data.substr(1) };
                        }
                        var lines = res.data.split("\r\n");
                        var index = 0;
                        var series = null;
                        var timestamp = null;
                        var value = 0.0;
                        var datapoints = [];
                        var currentTarget = null;
                        var series_names = [];
                        lodash_1.default.forEach(lines, function (line) {
                            var step = index % 3;
                            if (step === 0) {
                                // parse series name
                                series = line.substr(1);
                                if (series) {
                                    series_names.push(series);
                                }
                            }
                            index++;
                        });
                        var newTarget = {
                            metric: metricName,
                            tags: _this.extractTags(series_names),
                            shouldComputeRate: target.shouldComputeRate,
                            shouldEWMA: target.shouldEWMA,
                            decay: target.decay,
                            downsampleAggregator: target.downsampleAggregator
                        };
                        return _this.groupAggregateTargetQuery(begin, end, interval, limit, newTarget);
                    });
                };
                /** Query time-series storage */
                AkumuliDatasource.prototype.groupAggregateTargetQuery = function (begin, end, interval, limit, target) {
                    var metricName = target.metric;
                    var tags = target.tags;
                    var aggFunc = target.downsampleAggregator;
                    var rate = target.shouldComputeRate;
                    var ewma = target.shouldEWMA;
                    var decay = target.decay || 0.5;
                    var query = {
                        "group-aggregate": {
                            metric: metricName,
                            step: interval,
                            func: [aggFunc]
                        },
                        range: {
                            from: begin.format('YYYYMMDDTHHmmss.SSS'),
                            to: end.format('YYYYMMDDTHHmmss.SSS')
                        },
                        where: tags,
                        "order-by": "series",
                        apply: []
                    };
                    if (rate) {
                        query["apply"].push({ name: "rate" });
                    }
                    if (ewma) {
                        query["apply"].push({ name: "ewma-error", decay: decay });
                    }
                    var httpRequest = {
                        method: "POST",
                        url: this.instanceSettings.url + "/api/query",
                        data: query
                    };
                    // Read the actual data and process it
                    return this.backendSrv.datasourceRequest(httpRequest).then(function (res) {
                        var data = [];
                        if (res.status === 'error') {
                            throw res.error;
                        }
                        if (res.data.charAt(0) === '-') {
                            throw { message: res.data.substr(1) };
                        }
                        var lines = res.data.split("\r\n");
                        var index = 0;
                        var series = null;
                        var timestamp = null;
                        var value = 0.0;
                        var datapoints = [];
                        var currentTarget = null;
                        lodash_1.default.forEach(lines, function (line) {
                            var step = index % 4;
                            switch (step) {
                                case 0:
                                    // parse series name
                                    series = line.replace(/(\S*)(:mean)(.*)/g, "$1$3").substr(1);
                                    break;
                                case 1:
                                    // parse timestamp
                                    timestamp = moment_1.default.utc(line.substr(1)).local();
                                    break;
                                case 2:
                                    break;
                                case 3:
                                    value = parseFloat(line.substr(1));
                                    break;
                            }
                            if (step === 3) {
                                if (currentTarget == null) {
                                    currentTarget = series;
                                }
                                if (currentTarget === series) {
                                    datapoints.push([value, timestamp]);
                                }
                                else {
                                    data.push({
                                        target: currentTarget,
                                        datapoints: datapoints
                                    });
                                    datapoints = [[value, timestamp]];
                                    currentTarget = series;
                                }
                            }
                            index++;
                        });
                        if (datapoints.length !== 0) {
                            data.push({
                                target: currentTarget,
                                datapoints: datapoints
                            });
                        }
                        return data;
                    });
                };
                /** Query time-series storage */
                AkumuliDatasource.prototype.selectTopNQuery = function (begin, end, limit, target) {
                    var _this = this;
                    // Use all the same parametres as original query
                    // but add 'top' function to the 'apply' clause.
                    // Extract tags from results and run 'select' query
                    // nomrally.
                    var metricName = target.metric;
                    var tags = target.tags;
                    var isTop = target.topN ? true : false;
                    var topN = target.topN;
                    if (!isTop) {
                        throw "top-N parameter required";
                    }
                    var query = {
                        "select": metricName,
                        range: {
                            from: begin.format('YYYYMMDDTHHmmss.SSS'),
                            to: end.format('YYYYMMDDTHHmmss.SSS')
                        },
                        where: tags,
                        "order-by": "series",
                        apply: [{ name: "top", N: topN }]
                    };
                    var httpRequest = {
                        method: "POST",
                        url: this.instanceSettings.url + "/api/query",
                        data: query
                    };
                    return this.backendSrv.datasourceRequest(httpRequest).then(function (res) {
                        var data = [];
                        if (res.status === 'error') {
                            throw res.error;
                        }
                        if (res.data.charAt(0) === '-') {
                            throw { message: "Query error: " + res.data.substr(1) };
                        }
                        var lines = res.data.split("\r\n");
                        var index = 0;
                        var series = null;
                        var timestamp = null;
                        var value = 0.0;
                        var datapoints = [];
                        var currentTarget = null;
                        var series_names = [];
                        lodash_1.default.forEach(lines, function (line) {
                            var step = index % 3;
                            if (step === 0) {
                                // parse series name
                                series = line.substr(1);
                                if (series) {
                                    series_names.push(series);
                                }
                            }
                            index++;
                        });
                        var newTarget = {
                            metric: metricName,
                            tags: _this.extractTags(series_names),
                            shouldComputeRate: target.shouldComputeRate,
                            shouldEWMA: target.shouldEWMA,
                            decay: target.decay,
                        };
                        return _this.selectTargetQuery(begin, end, limit, newTarget);
                    });
                };
                /** Query time-series storage */
                AkumuliDatasource.prototype.selectTargetQuery = function (begin, end, limit, target) {
                    var metricName = target.metric;
                    var tags = target.tags;
                    var rate = target.shouldComputeRate;
                    var ewma = target.shouldEWMA;
                    var decay = target.decay || 0.5;
                    var query = {
                        "select": metricName,
                        range: {
                            from: begin.format('YYYYMMDDTHHmmss.SSS'),
                            to: end.format('YYYYMMDDTHHmmss.SSS')
                        },
                        where: tags,
                        "order-by": "series",
                        apply: []
                    };
                    if (rate) {
                        query["apply"].push({ name: "rate" });
                    }
                    if (ewma) {
                        query["apply"].push({ name: "ewma-error", decay: decay });
                    }
                    var httpRequest = {
                        method: "POST",
                        url: this.instanceSettings.url + "/api/query",
                        data: query
                    };
                    return this.backendSrv.datasourceRequest(httpRequest).then(function (res) {
                        var data = [];
                        if (res.status === 'error') {
                            throw res.error;
                        }
                        if (res.data.charAt(0) === '-') {
                            throw { message: "Query error: " + res.data.substr(1) };
                        }
                        var lines = res.data.split("\r\n");
                        var index = 0;
                        var series = null;
                        var timestamp = null;
                        var value = 0.0;
                        var datapoints = [];
                        var currentTarget = null;
                        lodash_1.default.forEach(lines, function (line) {
                            var step = index % 3;
                            switch (step) {
                                case 0:
                                    // parse series name
                                    series = line.substr(1);
                                    break;
                                case 1:
                                    // parse timestamp
                                    timestamp = moment_1.default.utc(line.substr(1)).local();
                                    break;
                                case 2:
                                    value = parseFloat(line.substr(1));
                                    break;
                            }
                            if (step === 2) {
                                if (currentTarget == null) {
                                    currentTarget = series;
                                }
                                if (currentTarget === series) {
                                    datapoints.push([value, timestamp]);
                                }
                                else {
                                    data.push({
                                        target: currentTarget,
                                        datapoints: datapoints
                                    });
                                    datapoints = [[value, timestamp]];
                                    currentTarget = series;
                                }
                            }
                            index++;
                        });
                        if (datapoints.length !== 0) {
                            data.push({
                                target: currentTarget,
                                datapoints: datapoints
                            });
                        }
                        return data;
                    });
                };
                AkumuliDatasource.prototype.query = function (options) {
                    var _this = this;
                    var begin = options.range.from.utc();
                    var end = options.range.to.utc();
                    var interval = options.interval;
                    var limit = options.maxDataPoints; // TODO: don't ignore the limit
                    var allQueryPromise = lodash_1.default.map(options.targets, function (target) {
                        var disableDownsampling = target.disableDownsampling;
                        var isTop = target.topN ? true : false;
                        if (disableDownsampling) {
                            if (isTop) {
                                return _this.selectTopNQuery(begin, end, limit, target);
                            }
                            else {
                                return _this.selectTargetQuery(begin, end, limit, target);
                            }
                        }
                        else {
                            if (isTop) {
                                return _this.groupAggregateTopNQuery(begin, end, interval, limit, target);
                            }
                            else {
                                return _this.groupAggregateTargetQuery(begin, end, interval, limit, target);
                            }
                        }
                    });
                    return this.$q.all(allQueryPromise).then(function (allResults) {
                        var data = [];
                        lodash_1.default.forEach(allResults, function (result, index) {
                            data = data.concat(result);
                        });
                        return { data: data };
                    });
                };
                return AkumuliDatasource;
            }());
            exports_1("AkumuliDatasource", AkumuliDatasource);
        }
    };
});
//# sourceMappingURL=datasource.js.map