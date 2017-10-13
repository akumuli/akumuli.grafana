///<reference path="../../../headers/common.d.ts" />

import _ from 'lodash';
import moment from "moment";

class AkumuliDatasource {

  /** @ngInject */
  constructor(private instanceSettings, private backendSrv, private $q) {}

  query(options) {
    console.log("Query:");
    console.log(options.targets);
    console.log(options.maxDataPoints);
    console.log(options.interval);
    console.log("-----");
    return this.timeSeriesQuery(options).then(res => {
      if (res.data.charAt(0) === '-') {
        console.log("Query error");
        return { data: null };
      }
      var data = [];
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
      return { data: data };
    });
  }

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

  metricFindQuery(metricName) {
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
      if (res.data.charAt(0) === '-') {
        console.log("Query error");
        return data;
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
      if (res.data.charAt(0) === '-') {
        console.log("Query error");
        return data;
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

  suggestTagValues(metric, tagName, valuePrefix) {
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
      if (res.data.charAt(0) === '-') {
        console.log("Query error");
        return data;
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

  /** Query time-series storage */
  timeSeriesQuery(options) {
    var begin    = options.range.from.utc();
    var end      = options.range.to.utc();
    var interval = options.interval;
    var limit    = options.maxDataPoints;
    console.log('timeSeriesQuery: ' + begin.format('YYYYMMDDThhmmss.SSS')
                                    +   end.format('YYYYMMDDThhmmss.SSS'));
    if (options.targets.length !== 1) {
      console.log("Only a signel target is supported at the moment");
      throw new Error("Only a signel target is supported at the moment");
    }
    var metricName = options.targets[0].metric;
    var tags       = options.targets[0].tags;
    var aggFunc    = options.targets[0].downsampleAggregator;
    var requestBody: any = {
      "group-aggregate": {
        metric: metricName,
        step: interval,
        func: [ aggFunc ]
      },
      range: {
        from: begin.format('YYYYMMDDThhmmss.SSS'),
        to: end.format('YYYYMMDDThhmmss.SSS')
      },
      where: tags,
      //limit: limit,
      "order-by": "series"
    };

    var httpRequest: any = {
      method: "POST",
      url: this.instanceSettings.url + "/api/query",
      data: requestBody
    };

    return this.backendSrv.datasourceRequest(httpRequest);
  }
}

export {AkumuliDatasource};
