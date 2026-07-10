import { HttpProxyAgent } from 'http-proxy-agent';
import { HttpsProxyAgent } from 'https-proxy-agent';
import { ProxyAgents, ProxySettings } from '.';

export function createProxyAgents(
  proxySettings: ProxySettings
): ProxyAgents | undefined {
  const { address, port, auth } = proxySettings;
  const proxyUrl = new URL(address);

  if (port) {
    proxyUrl.port = port.toString();
  }

  if (auth) {
    proxyUrl.username = auth.username;
    proxyUrl.password = auth.password;
  }

  return {
    httpAgent: new HttpProxyAgent(proxyUrl.toString()),
    httpsAgent: new HttpsProxyAgent(proxyUrl.toString()),
  };
}
