import * as Consumer from './consumer';
import * as Kafka from './kafka';

import { ConsumerModule } from './consumer';
import { KafkaModule } from './kafka';

const Modules = {
  KafkaModule,
  ConsumerModule,
};

const NestJsKafkaClient = {
  Modules,
  Consumer,
  Kafka,
};

export { Consumer, Kafka, Modules, NestJsKafkaClient };

export * from './consumer';
export * from './kafka';
