export interface EventMessage {
  key: Buffer | null;
  value: Buffer | null;
  timestamp: string;
  attributes: number;
  offset: string;
  headers: Record<string, Buffer>;
  size?: never;
}

export interface IEventHandler<T>  {
  /**
   * Handle a single Kafka event
   * @param params Event parameters including key, event data, and optional Kafka message
   */
  handle?(params: { key: string, event: T, payload?: EventMessage }): Promise<void>;
  
  /**
   * Optional batch handler for processing multiple events with the same key together
   * If implemented, the Kafka client may choose to use this for better efficiency
   * @param params Batch of events grouped by key
   */
  handleBatch?(params: { 
    key: string, 
    events: T[], 
    payloads?: EventMessage[] 
  }): Promise<void>;
}
