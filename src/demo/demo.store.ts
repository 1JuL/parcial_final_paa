export interface ProviderOverride {
  enabled: boolean;   // Si false, el proveedor lanza error inmediatamente
  slow:    boolean;   // Si true, el proveedor simula un timeout (>8s)
}

export interface DemoStore {
  clicksend: ProviderOverride;
  twilio:    ProviderOverride;
  logs:      DemoLogEntry[];
}

export interface DemoLogEntry {
  timestamp: string;    // ISO 8601
  level:     'info' | 'ok' | 'warn' | 'error';
  message:   string;
}

// Estado inicial: ambos proveedores activos
const store: DemoStore = {
  clicksend: { enabled: true, slow: false },
  twilio:    { enabled: true, slow: false },
  logs:      [],
};

export function getProviderOverride(provider: 'clicksend' | 'twilio'): ProviderOverride {
  return store[provider];
}

export function setProviderOverride(
  provider: 'clicksend' | 'twilio',
  override: Partial<ProviderOverride>,
): void {
  store[provider] = { ...store[provider], ...override };
}

export function addDemoLog(level: DemoLogEntry['level'], message: string): void {
  const entry: DemoLogEntry = {
    timestamp: new Date().toISOString(),
    level,
    message,
  };
  store.logs.push(entry);
  // Mantener solo los últimos 200 logs en memoria
  if (store.logs.length > 200) {
    store.logs.shift();
  }
}

export function getDemoLogs(): DemoLogEntry[] {
  return [...store.logs];
}

export function getDemoStore(): DemoStore {
  return store;
}
