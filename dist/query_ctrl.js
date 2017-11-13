///<reference path="../../../headers/common.d.ts" />
System.register(["lodash", "app/core/utils/kbn", "app/plugins/sdk"], function (exports_1, context_1) {
    "use strict";
    var __extends = (this && this.__extends) || (function () {
        var extendStatics = Object.setPrototypeOf ||
            ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
            function (d, b) { for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p]; };
        return function (d, b) {
            extendStatics(d, b);
            function __() { this.constructor = d; }
            d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
        };
    })();
    var __moduleName = context_1 && context_1.id;
    var lodash_1, kbn_1, sdk_1, AkumuliQueryCtrl;
    return {
        setters: [
            function (lodash_1_1) {
                lodash_1 = lodash_1_1;
            },
            function (kbn_1_1) {
                kbn_1 = kbn_1_1;
            },
            function (sdk_1_1) {
                sdk_1 = sdk_1_1;
            }
        ],
        execute: function () {///<reference path="../../../headers/common.d.ts" />
            AkumuliQueryCtrl = (function (_super) {
                __extends(AkumuliQueryCtrl, _super);
                /** @ngInject **/
                function AkumuliQueryCtrl($scope, $injector) {
                    var _this = _super.call(this, $scope, $injector) || this;
                    _this.errors = _this.validateTarget();
                    _this.aggregators = ['sum', 'min', 'max', 'mean', 'count'];
                    _this.fillPolicies = ['none', 'nan', 'null', 'zero'];
                    _this.filterTypes = ['wildcard', 'iliteral_or', 'not_iliteral_or', 'not_literal_or', 'iwildcard', 'literal_or', 'regexp'];
                    _this.tsdbVersion = _this.datasource.tsdbVersion;
                    if (!_this.target.aggregator) {
                        _this.target.aggregator = 'sum';
                    }
                    if (!_this.target.downsampleAggregator) {
                        _this.target.downsampleAggregator = 'mean';
                    }
                    if (!_this.target.downsampleFillPolicy) {
                        _this.target.downsampleFillPolicy = 'none';
                    }
                    _this.datasource.getAggregators().then(function (aggs) {
                        if (aggs.length !== 0) {
                            _this.aggregators = aggs;
                        }
                    });
                    // needs to be defined here as it is called from typeahead
                    _this.suggestMetrics = function (query, callback) {
                        _this.datasource.metricFindQuery(query)
                            .then(_this.getTextValues)
                            .then(callback);
                    };
                    _this.suggestTagKeys = function (query, callback) {
                        _this.datasource.suggestTagKeys(_this.target.metric, _this.target.currentTagKey)
                            .then(_this.getTextValues)
                            .then(callback);
                    };
                    _this.suggestTagValues = function (query, callback) {
                        _this.datasource.suggestTagValues(_this.target.metric, _this.target.currentTagKey, _this.target.currentTagValue)
                            .then(_this.getTextValues)
                            .then(callback);
                    };
                    return _this;
                }
                AkumuliQueryCtrl.prototype.targetBlur = function () {
                    this.errors = this.validateTarget();
                    this.refresh();
                };
                AkumuliQueryCtrl.prototype.getTextValues = function (metricFindResult) {
                    return lodash_1.default.map(metricFindResult, function (value) { return value.text; });
                };
                AkumuliQueryCtrl.prototype.addTag = function () {
                    if (this.target.filters && this.target.filters.length > 0) {
                        this.errors.tags = "Please remove filters to use tags, tags and filters are mutually exclusive.";
                    }
                    if (!this.addTagMode) {
                        this.addTagMode = true;
                        return;
                    }
                    if (!this.target.tags) {
                        this.target.tags = {};
                    }
                    this.errors = this.validateTarget();
                    if (!this.errors.tags) {
                        this.target.tags[this.target.currentTagKey] = this.target.currentTagValue;
                        this.target.currentTagKey = '';
                        this.target.currentTagValue = '';
                        this.targetBlur();
                    }
                    this.addTagMode = false;
                };
                AkumuliQueryCtrl.prototype.removeTag = function (key) {
                    delete this.target.tags[key];
                    this.targetBlur();
                };
                AkumuliQueryCtrl.prototype.editTag = function (key, value) {
                    this.removeTag(key);
                    this.target.currentTagKey = key;
                    this.target.currentTagValue = value;
                    this.addTag();
                };
                AkumuliQueryCtrl.prototype.closeAddTagMode = function () {
                    this.addTagMode = false;
                    return;
                };
                AkumuliQueryCtrl.prototype.addFilter = function () {
                    if (this.target.tags && lodash_1.default.size(this.target.tags) > 0) {
                        this.errors.filters = "Please remove tags to use filters, tags and filters are mutually exclusive.";
                    }
                    if (!this.addFilterMode) {
                        this.addFilterMode = true;
                        return;
                    }
                    if (!this.target.filters) {
                        this.target.filters = [];
                    }
                    if (!this.target.currentFilterType) {
                        this.target.currentFilterType = 'iliteral_or';
                    }
                    if (!this.target.currentFilterGroupBy) {
                        this.target.currentFilterGroupBy = false;
                    }
                    this.errors = this.validateTarget();
                    if (!this.errors.filters) {
                        var currentFilter = {
                            type: this.target.currentFilterType,
                            tagk: this.target.currentFilterKey,
                            filter: this.target.currentFilterValue,
                            groupBy: this.target.currentFilterGroupBy
                        };
                        this.target.filters.push(currentFilter);
                        this.target.currentFilterType = 'literal_or';
                        this.target.currentFilterKey = '';
                        this.target.currentFilterValue = '';
                        this.target.currentFilterGroupBy = false;
                        this.targetBlur();
                    }
                    this.addFilterMode = false;
                };
                AkumuliQueryCtrl.prototype.removeFilter = function (index) {
                    this.target.filters.splice(index, 1);
                    this.targetBlur();
                };
                AkumuliQueryCtrl.prototype.editFilter = function (fil, index) {
                    this.removeFilter(index);
                    this.target.currentFilterKey = fil.tagk;
                    this.target.currentFilterValue = fil.filter;
                    this.target.currentFilterType = fil.type;
                    this.target.currentFilterGroupBy = fil.groupBy;
                    this.addFilter();
                };
                AkumuliQueryCtrl.prototype.closeAddFilterMode = function () {
                    this.addFilterMode = false;
                    return;
                };
                AkumuliQueryCtrl.prototype.validateTarget = function () {
                    var errs = {};
                    if (this.target.shouldDownsample) {
                        try {
                            if (this.target.downsampleInterval) {
                                kbn_1.default.describe_interval(this.target.downsampleInterval);
                            }
                            else {
                                errs.downsampleInterval = "You must supply a downsample interval (e.g. '1m' or '1h').";
                            }
                        }
                        catch (err) {
                            errs.downsampleInterval = err.message;
                        }
                    }
                    if (this.target.tags && lodash_1.default.has(this.target.tags, this.target.currentTagKey)) {
                        errs.tags = "Duplicate tag key '" + this.target.currentTagKey + "'.";
                    }
                    return errs;
                };
                return AkumuliQueryCtrl;
            }(sdk_1.QueryCtrl));
            AkumuliQueryCtrl.templateUrl = 'partials/query.editor.html';
            exports_1("AkumuliQueryCtrl", AkumuliQueryCtrl);
        }
    };
});
//# sourceMappingURL=query_ctrl.js.map