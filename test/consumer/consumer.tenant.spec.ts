import { createMock } from '@golevelup/ts-jest';
import { ConsumerService } from '../../src/consumer/consumer.service';
import { IEventHandler, KafkaClient } from '@this/kafka';
import { ModuleRef } from '@nestjs/core';
import { ConsumerRefService } from '@this/consumer/consumer.ref';

describe('ConsumerService multi-tenant fan-out', () => {
  const moduleRef = createMock<ModuleRef>();
  const consumerRef = createMock<ConsumerRefService>();
  const handler = createMock<IEventHandler<any>>();
  const def = { topic: '{tenant}.cases.case-created', handler };

  afterEach(() => jest.clearAllMocks());

  it('single group when no tenants configured', async () => {
    const kafkaClient = createMock<KafkaClient>();
    const svc = new ConsumerService('wallet', kafkaClient, consumerRef, moduleRef);
    await svc.consumeMany([def]);

    expect(kafkaClient.consumeMany).toHaveBeenCalledTimes(1);
    expect(kafkaClient.consumeMany).toHaveBeenCalledWith(
      [{ topic: '{tenant}.cases.case-created', handler }],
      'wallet-consumer',
    );
  });

  it('one group per tenant with {tenant} substituted in topic and group', async () => {
    const kafkaClient = createMock<KafkaClient>();
    const svc = new ConsumerService('wallet', kafkaClient, consumerRef, moduleRef, {
      tenants: ['latam', 'avianca'],
    });
    await svc.consumeMany([def]);

    expect(kafkaClient.consumeMany).toHaveBeenCalledTimes(2);
    expect(kafkaClient.consumeMany).toHaveBeenNthCalledWith(
      1,
      [{ topic: 'latam.cases.case-created', handler }],
      'latam.wallet',
    );
    expect(kafkaClient.consumeMany).toHaveBeenNthCalledWith(
      2,
      [{ topic: 'avianca.cases.case-created', handler }],
      'avianca.wallet',
    );
  });

  it('honors a custom groupId template', async () => {
    const kafkaClient = createMock<KafkaClient>();
    const svc = new ConsumerService('wallet', kafkaClient, consumerRef, moduleRef, {
      tenants: ['latam'],
      groupId: '{tenant}.{name}.issue-cards',
    });
    await svc.consumeMany([def]);

    expect(kafkaClient.consumeMany).toHaveBeenCalledWith(
      [{ topic: 'latam.cases.case-created', handler }],
      'latam.wallet.issue-cards',
    );
  });
});
