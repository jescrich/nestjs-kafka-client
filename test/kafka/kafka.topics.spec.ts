import { KafkaTopics } from '../../src/kafka/kafka.topics';

describe('KafkaTopics', () => {
  it('builds a tenant-scoped event topic', () => {
    expect(KafkaTopics.event({ scope: 'latam', domain: 'cases', event: 'case-created' })).toBe(
      'latam.cases.case-created',
    );
  });

  it('builds a canonical consumer group', () => {
    expect(
      KafkaTopics.consumerGroup({ tenant: 'latam', service: 'wallet', purpose: 'issue-cards' }),
    ).toBe('latam.wallet.issue-cards');
  });

  it('derives dlq and retry topics', () => {
    const base = KafkaTopics.event({ scope: 'latam', domain: 'cases', event: 'case-created' });
    expect(KafkaTopics.dlq(base)).toBe('latam.cases.case-created-dlq');
    expect(KafkaTopics.dlq(base, '.dlq')).toBe('latam.cases.case-created.dlq');
    expect(KafkaTopics.retry(base, 1)).toBe('latam.cases.case-created.retry.1');
  });

  it('substitutes the {tenant} placeholder', () => {
    expect(KafkaTopics.withTenant('{tenant}.cases.case-created', 'latam')).toBe(
      'latam.cases.case-created',
    );
    expect(KafkaTopics.hasTenantPlaceholder('{tenant}.cases.x')).toBe(true);
    expect(KafkaTopics.hasTenantPlaceholder('latam.cases.x')).toBe(false);
  });

  it('rejects invalid segments (dots, empty, uppercase)', () => {
    expect(() => KafkaTopics.event({ scope: 'la.tam', domain: 'cases', event: 'x' })).toThrow();
    expect(() => KafkaTopics.event({ scope: '', domain: 'cases', event: 'x' })).toThrow();
    expect(() => KafkaTopics.consumerGroup({ tenant: 'LATAM', service: 'w', purpose: 'p' })).toThrow();
  });
});
