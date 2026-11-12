// A minimal circuit breaker: opens after N consecutive failures, rejects calls while
// open, then lets one trial call through ("half-open") after a cool-down.
export class CircuitOpenError extends Error {
  constructor() {
    super("circuit breaker is open");
  }
}

export class CircuitBreaker {
  private failures = 0;
  private openUntil = 0;

  constructor(private opts: { failureThreshold: number; openMs: number }) {}

  async call<T>(fn: () => Promise<T>, isSuccess: (e: unknown) => boolean = () => false): Promise<T> {
    if (Date.now() < this.openUntil) throw new CircuitOpenError();
    try {
      const result = await fn();
      this.failures = 0;
      return result;
    } catch (e) {
      if (isSuccess(e)) {
        this.failures = 0; // e.g. a 404: the dependency answered correctly
      } else if (++this.failures >= this.opts.failureThreshold) {
        this.openUntil = Date.now() + this.opts.openMs;
        this.failures = 0;
      }
      throw e;
    }
  }
}
