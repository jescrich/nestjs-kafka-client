/**
 * Canonical topic / consumer-group naming for multi-tenant event-driven systems.
 *
 * Tenant-scoped event topics are `<scope>.<domain>.<event>` (the scope is the tenant, e.g.
 * `latam.cases.case-created`); consumer groups are `<tenant>.<service>.<purpose>`. This mirrors
 * the convention used by `@nexa/messaging` so a per-tenant consumer fleet can be named
 * consistently instead of every service hand-rolling string templates.
 */

const SEGMENT = /^[a-z0-9-]+$/;

function assertSegment(value: string, field: string): string {
  if (!value || !SEGMENT.test(value)) {
    throw new Error(
      `KafkaTopics: invalid ${field} "${value}" — must be non-empty and match ${SEGMENT} (no dots).`,
    );
  }
  return value;
}

export interface EventTopicArgs {
  /** Tenant / scope segment, e.g. an airline code, or `platform` for cross-tenant topics. */
  scope: string;
  /** Business domain, e.g. `cases`, `booking`, `rebooking`. */
  domain: string;
  /** Event name, e.g. `case-created`, `options-generated`. */
  event: string;
}

export interface ConsumerGroupArgs {
  /** Tenant the group belongs to. */
  tenant: string;
  /** Service that owns the group, e.g. `wallet`, `data-platform`. */
  service: string;
  /** What the group does, e.g. `issue-cards`, `warehouse-ingest`. Disambiguates groups in one service. */
  purpose: string;
}

export class KafkaTopics {
  /** `<scope>.<domain>.<event>` — a tenant-scoped (or `platform.*`) event topic. */
  static event({ scope, domain, event }: EventTopicArgs): string {
    return `${assertSegment(scope, 'scope')}.${assertSegment(domain, 'domain')}.${assertSegment(event, 'event')}`;
  }

  /** `<tenant>.<service>.<purpose>` — a canonical consumer-group id. */
  static consumerGroup({ tenant, service, purpose }: ConsumerGroupArgs): string {
    return `${assertSegment(tenant, 'tenant')}.${assertSegment(service, 'service')}.${assertSegment(purpose, 'purpose')}`;
  }

  /** `<base><suffix>` — the dead-letter topic for `base`. Suffix defaults to `-dlq`. */
  static dlq(base: string, suffix = '-dlq'): string {
    return `${base}${suffix}`;
  }

  /** `<base>.retry.<n>` — the nth retry-tier topic for `base`. */
  static retry(base: string, n: number): string {
    return `${base}.retry.${n}`;
  }

  /**
   * Substitute the `{tenant}` placeholder in a topic or group template. Used by the per-tenant
   * consumer fan-out so a single `@Handler({ topic: '{tenant}.cases.case-created' })` expands to
   * one concrete topic per registered tenant.
   */
  static withTenant(template: string, tenant: string): string {
    return template.replace(/\{tenant\}/g, tenant);
  }

  /** True when `template` contains a `{tenant}` placeholder. */
  static hasTenantPlaceholder(template: string): boolean {
    return template.includes('{tenant}');
  }
}
