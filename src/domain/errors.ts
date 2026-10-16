export class DomainError extends Error {
  constructor(public readonly code: string) {
    super(code);
    this.name = 'DomainError';
  }
}

export class ApplicationError extends Error {
  constructor(
    public readonly code: string,
    public readonly httpStatus: number,
  ) {
    super(code);
    this.name = 'ApplicationError';
  }
}

export type FailureCode =
  | 'INSUFFICIENT_FUNDS'
  | 'REVERSAL_INSUFFICIENT_FUNDS'
  | 'CURRENCY_MISMATCH'
  | 'PLAYER_MISMATCH'
  | 'REFERENCE_MISMATCH'
  | 'REFERENCE_NOT_PROCESSED'
  | 'REFERENCE_KIND_INVALID'
  | 'REFERENCE_AMOUNT_MISMATCH'
  | 'REFERENCE_ALREADY_REVERSED'
  | 'REFERENCE_NOT_FOUND'
  | 'WALLET_CAPACITY_EXCEEDED'
  | 'PERMANENT_INFRASTRUCTURE_ERROR';
