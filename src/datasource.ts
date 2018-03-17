///<reference path="../node_modules/grafana-sdk-mocks/app/headers/common.d.ts" />

import _ from 'lodash';
import moment from "moment";

class AkumuliDatasource {

  /** @ngInject */
  constructor(private instanceSettings, private backendSrv, private templateSrv, private $q) {}

  /** Test that datasource connection works */
  testDatasource() {
    var options: any = {
      method: "GET",
      url: this.instanceSettings.url + "/api/stats",
      data: ""
    };
    return this.backendSrv.datasourceRequest(options).then(res => {
      return { status: "success", message: "Data source is working", title: "Success" };
    });
  }

  metricFindQuery(queryString) {
    var components = queryString.split(" ");
    var len = components.length;
    if (len == 0) {
      // query metric names
      return this.suggestMetricNames("");
    } else if (len == 1) {
      // query tag names
      return this.suggestTagKeys(components[0], "");
    } else if (len == 2) {
      // query tag values
      return this.suggestTagValues(components[0], components[1], "", false);
    }
    throw { message: "Invalid query string (up too three components can be used)" };
  }

  suggestMetricNames(metricName) {
    var requestBody: any = {
      select: "metric-names",
      "starts-with": metricName
    };
    var httpRequest: any = {
      method: "POST",
      url: this.instanceSettings.url + "/api/suggest",
      data: requestBody
    };

    return this.backendSrv.datasourceRequest(httpRequest).then(res => {
      var data = [];
      if (res.status === 'error') {
        throw res.error;
      }
      if (res.data.charAt(0) === '-') {
        throw { message: res.data.substr(1) };
      }
      var lines = res.data.split("\r\n");
      _.forEach(lines, line => {
        if (line) {
          var name = line.substr(1);
          data.push({text: name, value: name});
        }
      });
      return data;
    });
  }

  /** Parse series name in a canonical form */
  extractTags(names) {
    var where = [];
    _.forEach(names, name => {
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
  }

  annotationQuery(options) {
    return this.backendSrv.get('/api/annotations', {
      from: options.range.from.valueOf(),
      to: options.range.to.valueOf(),
      limit: options.limit,
      type: options.type,
    });
  }

  getAggregators() {
    // TODO: query aggregators from Akumuli
    return new Promise((resolve, reject) => {
      resolve(["mean", "sum", "count", "min", "max"]);
    });
  }

  suggestTagKeys(metric, tagPrefix) {
    tagPrefix = tagPrefix || "";
    var requestBody: any = {
      select: "tag-names",
      metric: metric,
      "starts-with": tagPrefix
    };
    var httpRequest: any = {
      method: "POST",
      url: this.instanceSettings.url + "/api/suggest",
      data: requestBody
    };

    return this.backendSrv.datasourceRequest(httpRequest).then(res => {
      var data = [];
      if (res.status === 'error') {
        throw res.error;
      }
      if (res.data.charAt(0) === '-') {
        throw { message: res.data.substr(1) };
      }
      var lines = res.data.split("\r\n");
      _.forEach(lines, line => {
        if (line) {
          var name = line.substr(1);
          data.push({text: name, value: name});
        }
      });
      return data;
    });
  }

  suggestTagValues(metric, tagName, valuePrefix, addTemplateVars) {
    tagName = tagName || "";
    valuePrefix = valuePrefix || "";
    var requestBody: any = {
      select: "tag-values",
      metric: metric,
      tag: tagName,
      "starts-with": valuePrefix
    };
    var httpRequest: any = {
      method: "POST",
      url: this.instanceSettings.url + "/api/suggest",
      data: requestBody
    };

    return this.backendSrv.datasourceRequest(httpRequest).then(res => {
      var data = [];
      if (res.status === 'error') {
        throw res.error;
      }
      if (res.data.charAt(0) === '-') {
        throw { message: res.data.substr(1) };
      }
      var lines = res.data.split("\r\n");
      _.forEach(lines, line => {
        if (line) {
          var name = line.substr(1);
          data.push({text: name, value: name});
        }
      });
      // Include template variables (if any)
      if (addTemplateVars) {
        _.forEach(Object.keys(this.templateSrv.index), varName => {
          var variable = this.templateSrv.index[varName];
          if (variable.type == "query") {
            var template = "$".concat(variable.name);
            data.push({text: template, value: template});
          }
        });
      }
      return data;
    });
  }

  /** Query time-series storage */
  groupAggregateTopNQuery(begin, end, interval, limit, target) {
    // Use all the same parametres as original query
    // but add 'top' function to the 'apply' clause.
    // Extract tags from results and run 'select' query
    // nomrally.
    var metricName = target.metric;
    var tags = {};
    if (target.tags) {
      _.forEach(Object.keys(target.tags), key => {
        var value = target.tags[key];
        value = this.templateSrv.replace(value);
        tags[key] = value;
      });
    }
    var isTop = target.topN ? true : false;
    var topN = target.topN;
    if (!isTop) {
      throw "top-N parameter required";
    }
    var query: any = {
      select: metricName,
      range: {
        from: begin.format('YYYYMMDDTHHmmss.SSS'),
        to: end.format('YYYYMMDDTHHmmss.SSS')
      },
      where: tags,
      "order-by": "series",
      apply: [{name: "top", N: topN}]
    };

    var httpRequest: any = {
      method: "POST",
      url: this.instanceSettings.url + "/api/query",
      data: query
    };

    return this.backendSrv.datasourceRequest(httpRequest).then(res => {
      if (res.status === 'error') {
        throw res.error;
      }
      if (res.data.charAt(0) === '-') {
        throw { message: "Query error: " + res.data.substr(1) };
      }
      var lines = res.data.split("\r\n");
      var index = 0;
      var series = null;
      var series_names = [];
      _.forEach(lines, line => {
        let step = index % 3;
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
        tags: this.extractTags(series_names),
        shouldComputeRate: target.shouldComputeRate,
        shouldEWMA: target.shouldEWMA,
        decay: target.decay,
        downsampleAggregator: target.downsampleAggregator,
        downsampleInterval: target.downsampleInterval,
      };
      return this.groupAggregateTargetQuery(begin, end, interval, limit, newTarget);
    });
  }
  /** Query time-series storage */
  groupAggregateTargetQuery(begin, end, interval, limit, target) {
    var metricName = target.metric;
    var tags = {};
    if (target.tags) {
      if (target.tags instanceof Array) {
        // Special case, TopN query is processed
        tags = target.tags;
      } else {
        _.forEach(Object.keys(target.tags), key => {
          var value = target.tags[key];
          value = this.templateSrv.replace(value);
          tags[key] = value;
        });
      }
    }
    var aggFunc = target.downsampleAggregator;
    var rate = target.shouldComputeRate;
    var ewma = target.shouldEWMA;
    var decay = target.decay || 0.5;
    var samplingInterval = target.downsampleInterval || interval
    var query: any = {
      "group-aggregate": {
        metric: metricName,
        step: samplingInterval,
        func: [ aggFunc ]
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
      query["apply"].push({name: "rate"});
    }
    if (ewma) {
      query["apply"].push({name: "ewma-error", decay: decay});
    }

    var httpRequest: any = {
      method: "POST",
      url: this.instanceSettings.url + "/api/query",
      data: query,
    };

    // Read the actual data and process it
    return this.backendSrv.datasourceRequest(httpRequest).then(res => {
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
      _.forEach(lines, line => {
        let step = index % 4;
        switch (step) {
          case 0:
            // parse series name
            series = line.replace(/(\S*)(:mean)(.*)/g, "$1$3").substr(1);
            break;
          case 1:
            // parse timestamp
            timestamp = moment.utc(line.substr(1)).local();
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
          } else {
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
  }

  /** Query time-series storage */
  selectTopNQuery(begin, end, limit, target) {
    // Use all the same parametres as original query
    // but add 'top' function to the 'apply' clause.
    // Extract tags from results and run 'select' query
    // nomrally.
    var metricName = target.metric;
    var tags = {};
    if (target.tags) {
      _.forEach(Object.keys(target.tags), key => {
        var value = target.tags[key];
        value = this.templateSrv.replace(value);
        tags[key] = value;
      });
    }
    var isTop = target.topN ? true : false;
    var topN = target.topN;
    if (!isTop) {
      throw "top-N parameter required";
    }
    var query: any = {
      "select": metricName,
      range: {
        from: begin.format('YYYYMMDDTHHmmss.SSS'),
        to: end.format('YYYYMMDDTHHmmss.SSS')
      },
      where: tags,
      "order-by": "series",
      apply: [{name: "top", N: topN}]
    };

    var httpRequest: any = {
      method: "POST",
      url: this.instanceSettings.url + "/api/query",
      data: query
    };

    return this.backendSrv.datasourceRequest(httpRequest).then(res => {
      if (res.status === 'error') {
        throw res.error;
      }
      if (res.data.charAt(0) === '-') {
        throw { message: "Query error: " + res.data.substr(1) };
      }
      var lines = res.data.split("\r\n");
      var index = 0;
      var series = null;
      var series_names = [];
      _.forEach(lines, line => {
        let step = index % 3;
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
        tags: this.extractTags(series_names),
        shouldComputeRate: target.shouldComputeRate,
        shouldEWMA: target.shouldEWMA,
        decay: target.decay,
      };
      return this.selectTargetQuery(begin, end, limit, newTarget);
    });
  }

  /** Query time-series storage */
  selectTargetQuery(begin, end, limit, target) {
    var metricName = target.metric;
    var tags = {};
    if (target.tags) {
      if (target.tags instanceof Array) {
        // Special case, TopN query is processed
        tags = target.tags;
      } else {
        _.forEach(Object.keys(target.tags), key => {
          var value = target.tags[key];
          value = this.templateSrv.replace(value);
          tags[key] = value;
        });
      }
    }
    var rate = target.shouldComputeRate;
    var ewma = target.shouldEWMA;
    var decay = target.decay || 0.5;
    var query: any = {
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
      query["apply"].push({name: "rate"});
    }
    if (ewma) {
      query["apply"].push({name: "ewma-error", decay: decay});
    }
    var httpRequest: any = {
      method: "POST",
      url: this.instanceSettings.url + "/api/query",
      data: query
    };

    return this.backendSrv.datasourceRequest(httpRequest).then(res => {
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
      _.forEach(lines, line => {
        let step = index % 3;
        switch (step) {
          case 0:
            // parse series name
            series = line.substr(1);
            break;
          case 1:
            // parse timestamp
            timestamp = moment.utc(line.substr(1)).local();
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
          } else {
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
  }


  query(options) {
    var begin    = options.range.from.utc();
    var end      = options.range.to.utc();
    var interval = options.interval;
    var limit    = options.maxDataPoints;  // TODO: don't ignore the limit
    var allQueryPromise = _.map(options.targets, target => {
      if (target.hide === true) {
        return new Promise((resolve, reject) => {
          resolve([]);
        });
      }
      var disableDownsampling = target.disableDownsampling;
      var isTop = target.topN ? true : false;
      if (disableDownsampling) {
        if (isTop) {
          return this.selectTopNQuery(begin, end, limit, target);
        } else {
          return this.selectTargetQuery(begin, end, limit, target);
        }
      } else {
        if (isTop) {
          return this.groupAggregateTopNQuery(begin, end, interval, limit, target);
        } else {
          return this.groupAggregateTargetQuery(begin, end, interval, limit, target);
        }
      }
    });

    return this.$q.all(allQueryPromise).then(allResults => {
      var data = [];
      _.forEach(allResults, (result, index) => {
        data = data.concat(result);
      })  ;
      return { data: data };
    });
  }
}

export {AkumuliDatasource};