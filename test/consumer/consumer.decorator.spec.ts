import { Test, TestingModule } from '@nestjs/testing';
import { ConsumerModule } from '../../src/consumer/consumer.module';
import { ConsumerService } from '../../src/consumer/consumer.service';
import { Handler } from '../../src/consumer/consumer.decorator';
import { EventMessage, IEventHandler, KafkaClient } from '@this/kafka';
import { DynamicModule, Injectable, OnModuleInit } from '@nestjs/common';
import { createMock } from "@golevelup/ts-jest";

// Mock event handler class with Consumer decorator
@Handler({ topic: 'test-topic' })
class TestConsumer {
  async handle(message: any) {
    return message;
  }
}

@Injectable()
class DummyService {
  saySomething(text: string) {
    return text;
  }
}

@Injectable()
@Handler({ topic: 'injectable-test-topic' })
class InjetableTestConsumer implements IEventHandler<any> {
  constructor(private readonly service: DummyService) {}
  handle(params: { key: string; event: any; payload?: EventMessage }): Promise<void> {
    const { key } = params;
    this.service.saySomething(key);
    return Promise.resolve();
  }
}

describe('Consumer Decorator', () => {
  let module: TestingModule;
  let consumerService: ConsumerService;
  let kafkaClient: KafkaClient;
  let consumerModule: DynamicModule;

  beforeEach(async () => {
    const mockKafkaClient = {
      consumeMany: jest.fn(),
    };

    consumerModule = ConsumerModule.register({
      name: 'test-app',
      brokers: 'localhost:9092',
      consumers: [TestConsumer],
    });

    module = await Test.createTestingModule({
      imports: [consumerModule],
      providers: [
        {
          provide: KafkaClient,
          useValue: mockKafkaClient,
        },
      ],
    })
      .overrideProvider(KafkaClient)
      .useValue(mockKafkaClient)
      .compile();

    consumerService = module.get<ConsumerService>(ConsumerService);
    kafkaClient = module.get<KafkaClient>(KafkaClient);
  });

  it('should use injected service in consumer', async () => {

    consumerModule = ConsumerModule.register({
      name: 'test-app',
      brokers: 'localhost:9092',
      consumers: [InjetableTestConsumer],
      providers: [DummyService],
    });

    module = await Test.createTestingModule({
      imports: [consumerModule],
      providers: [
        {
          provide: KafkaClient,
          useValue: createMock<KafkaClient>(),
        },
      ],
    }).compile();

    const service = module.get<DummyService>(DummyService);
    const consumer = module.get<InjetableTestConsumer>(InjetableTestConsumer);

    expect(consumer).toBeDefined();
    expect(service).toBeDefined();
    expect(consumer.handle).toBeDefined();

  });

  it('should properly decorate consumer class with topic metadata', () => {
    expect(Reflect.hasOwnMetadata('topic', TestConsumer)).toBeTruthy();
    expect(Reflect.hasOwnMetadata('topic-consumer', TestConsumer)).toBeTruthy();
    expect(Reflect.getMetadata('topic-consumer', TestConsumer)).toBe(true);
  });

  it('should discover and initialize consumer on module init', async () => {
    const x = module.get<OnModuleInit>(ConsumerService);
    x.onModuleInit();
    expect(kafkaClient.consumeMany).toHaveBeenCalledWith(
      [{ topic: 'test-topic', handler: expect.any(TestConsumer) }],
      'test-app-consumer',
    );
  });

  it('should register multiple consumers correctly', async () => {
    @Handler({ topic: 'another-topic' })
    class AnotherConsumer {
      async handle(message: any) {
        return message;
      }
    }

    const moduleWithMultipleConsumers = await Test.createTestingModule({
      imports: [
        ConsumerModule.register({
          name: 'test-app',
          brokers: 'localhost:9092',
          consumers: [TestConsumer, AnotherConsumer],
        }),
      ],
    })
      .overrideProvider(KafkaClient)
      .useValue({ consumeMany: jest.fn() })
      .compile();

    await moduleWithMultipleConsumers.init();

    const kafkaClientMultiple = moduleWithMultipleConsumers.get<KafkaClient>(KafkaClient);
    expect(kafkaClientMultiple.consumeMany).toHaveBeenCalledTimes(1);
    expect(kafkaClientMultiple.consumeMany).toHaveBeenCalledWith(
      expect.arrayContaining([
        { topic: 'test-topic', handler: expect.any(TestConsumer) },
        { topic: 'another-topic', handler: expect.any(AnotherConsumer) }
      ]),
      'test-app-consumer'
    );
  });
});
