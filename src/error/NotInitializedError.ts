export class NotInitializedError extends Error {
  constructor() {
    super();

    this.message = 'Module initialization must be preceded.';
  }
}
