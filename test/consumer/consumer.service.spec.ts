import { DeepMocked, createMock } from '@golevelup/ts-jest';
import { ConsumerService } from '../../src/consumer/consumer.service';
import { IEventHandler, KafkaClient } from '@this/kafka';
import { ModuleRef } from '@nestjs/core';
import { ConsumerRefService } from '@this/consumer/consumer.ref';

describe('ConsumerService', () => {
    let kafkaClient: KafkaClient;
    let consumerService: ConsumerService;
    let moduleRef = createMock<ModuleRef>();
    let consumerRef = createMock<ConsumerRefService>();
    const handlerMock: IEventHandler<any> = createMock<IEventHandler<any>>();
    
    beforeEach(() => {
        kafkaClient = new KafkaClient();
        consumerService = new ConsumerService('test-consumer', kafkaClient, consumerRef, moduleRef);
    });

    it('should be defined', () => {
        expect(consumerService).toBeDefined();
    });

    it('should have a name property', () => {
        expect(consumerService['name']).toBe('test-consumer');
    });

    it('should have a kafkaClient property', () => {
        expect(consumerService['kafkaClient']).toBe(kafkaClient);
    });

    afterEach(() => {
        jest.clearAllMocks();
    });    
});

