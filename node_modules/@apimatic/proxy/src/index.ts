export { createProxyAgents } from './proxyAgent';

export interface ProxyAgents {
  httpAgent: any;
  httpsAgent: any;
}

export interface ProxySettings {
  address: string;
  port?: number;
  auth?: {
    username: string;
    password: string;
  };
}
