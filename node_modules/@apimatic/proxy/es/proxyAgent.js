import { HttpProxyAgent } from 'http-proxy-agent';
import { HttpsProxyAgent } from 'https-proxy-agent';
function createProxyAgents(proxySettings) {
  var address = proxySettings.address,
    port = proxySettings.port,
    auth = proxySettings.auth;
  var proxyUrl = new URL(address);
  if (port) {
    proxyUrl.port = port.toString();
  }
  if (auth) {
    proxyUrl.username = auth.username;
    proxyUrl.password = auth.password;
  }
  return {
    httpAgent: new HttpProxyAgent(proxyUrl.toString()),
    httpsAgent: new HttpsProxyAgent(proxyUrl.toString())
  };
}
export { createProxyAgents };