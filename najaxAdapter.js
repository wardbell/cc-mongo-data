/**
 * A Breeze ajax adapter that uses the njax module to make jQuery.ajax-like XHR requests
 * See https://github.com/alanclarke/najax
 * Doesn't do everything a breeze ajax adapter should but it does enough for this app
 * Created by Ward on 1/14/14.
 */

var breeze = require('./breeze.debug');
var core = breeze.core;
var najax;

var ctor = function () {
    this.name = "najax";
    this.defaultSettings = {};
};

ctor.prototype.initialize = function () {
    najax = require('najax');
};

ctor.prototype.ajax = function (config) {
    if (!najax) {
        throw new Error("Unable to locate najax");
    }
    var jqConfig = {
        type: config.type,
        url: config.url,
        data: config.params || config.data,
        dataType: config.dataType,
        contentType: config.contentType,
        crossDomain: config.crossDomain
    }

    if (!core.isEmpty(this.defaultSettings)) {
        var compositeConfig = core.extend({}, this.defaultSettings);
        jqConfig = core.extend(compositeConfig, jqConfig);
    }

    jqConfig.success = function (data) {
        var httpResponse = {
            data: data,
            status: 200, // assume it
            getHeaders: function(){return ""},
            config: config
        };
        config.success(httpResponse);

    };
    jqConfig.error = function (error) {
        var httpResponse = {
            data: "", // ToDo: get from error
            status: 500, // Todo: get real status code
            getHeaders: function(){return ""},
            error: error,
            config: config
        };
        config.error(httpResponse);
    };
    /*
    jqConfig.error = function (XHR, textStatus, errorThrown) {
        var httpResponse = {
            data: XHR.responseText,
            status: XHR.status,
            getHeaders: getHeadersFn(XHR),
            error: errorThrown,
            config: config
        };
        config.error(httpResponse);
    };
    */
    najax(jqConfig);

};


function getHeadersFn(XHR) {
    return function (headerName) {
        if (headerName && headerName.length > 0) {
            return XHR.getResponseHeader(headerName);
        } else {
            return XHR.getAllResponseHeaders();
        };
    };
}


breeze.config.registerAdapter("ajax", ctor);

