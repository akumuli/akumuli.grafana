///<reference path="../../../headers/common.d.ts" />

import angular from 'angular';
import _ from 'lodash';

export class AkumuliConfigCtrl {
  static templateUrl = 'public/app/plugins/datasource/akumuli-datasource/partials/config.html';
  current: any;

  /** @ngInject */
  constructor($scope) {
    this.current.jsonData = this.current.jsonData || {};
    this.current.jsonData.tsdbVersion = this.current.jsonData.tsdbVersion || 1;
  }

  tsdbVersions = [
    {name: '==0.7', value: 1},
  ];

}
