/**
 * Decorator that marks a class as a consumer for a specific topic.
 * 
 * @param params - An object containing the topic name.
 * @param params.topic - The name of the topic to consume.
 * 
 * @returns A class decorator function that sets metadata for the topic consumer.
 */
const Handler = (params: { topic: string }) => (target: Function) => {
    const { topic } = params;
    Reflect.defineMetadata('topic-consumer', true, target);
    Reflect.defineMetadata('topic', topic, target);    
}

export { Handler }