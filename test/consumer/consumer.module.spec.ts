import { Test, TestingModule } from '@nestjs/testing';
import { ConsumerModule } from '../../src/consumer/consumer.module';
import { ConsumerService } from '../../src/consumer/consumer.service';

describe('ConsumerModule', () => {
    let consumerService: ConsumerService;

    beforeEach(async () => {
        const module: TestingModule = await Test.createTestingModule({
            imports: [
                ConsumerModule.register({
                    name: 'test-consumer',
                    brokers: 'test-broker',
                    consumers: []
                }),
            ],
        }).compile();

        consumerService = module.get<ConsumerService>(ConsumerService);
    });

    it('should be defined', () => {
        expect(consumerService).toBeDefined();
    });

    
});