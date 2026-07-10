import { ProxyAgents, ProxySettings } from '.';

export function createProxyAgents(
  _proxySettings: ProxySettings
): ProxyAgents | undefined {
  // tslint:disable-next-line:no-console
  console.warn('Proxy agents are not supported in browser environment');
  return undefined;
}
