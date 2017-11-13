/// <reference path="../../../../../public/app/headers/common.d.ts" />
export declare class AkumuliConfigCtrl {
    static templateUrl: string;
    current: any;
    /** @ngInject */
    constructor($scope: any);
    tsdbVersions: {
        name: string;
        value: number;
    }[];
}
