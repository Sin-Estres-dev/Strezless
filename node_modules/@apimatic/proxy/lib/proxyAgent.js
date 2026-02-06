"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.createProxyAgents = void 0;
var http_proxy_agent_1 = require("http-proxy-agent");
var https_proxy_agent_1 = require("https-proxy-agent");
function createProxyAgents(proxySettings) {
    var address = proxySettings.address, port = proxySettings.port, auth = proxySettings.auth;
    var proxyUrl = new URL(address);
    if (port) {
        proxyUrl.port = port.toString();
    }
    if (auth) {
        proxyUrl.username = auth.username;
        proxyUrl.password = auth.password;
    }
    return {
        httpAgent: new http_proxy_agent_1.HttpProxyAgent(proxyUrl.toString()),
        httpsAgent: new https_proxy_agent_1.HttpsProxyAgent(proxyUrl.toString()),
    };
}
exports.createProxyAgents = createProxyAgents;
