import { IEventHandler } from "@this/kafka";

/**
 * Represents a consumer definition for handling events.
 *
 * @template T - The type of the event data.
 */
export class ConsumerDef<T> {
    /**
     * The topic that the consumer listens to.
     */
    topic: string;

    /**
     * The handler that processes the event data.
     */
    handler: IEventHandler<T>;
}