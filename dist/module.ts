import {AkumuliDatasource} from './datasource';
import {AkumuliQueryCtrl} from './query_ctrl';
import {AkumuliConfigCtrl} from './config_ctrl';

class AnnotationsQueryCtrl {
  static templateUrl = 'partials/annotations.editor.html';
}

export {
  AkumuliDatasource as Datasource,
  AkumuliQueryCtrl as QueryCtrl,
  AkumuliConfigCtrl as ConfigCtrl,
  AnnotationsQueryCtrl as AnnotationsQueryCtrl
};
