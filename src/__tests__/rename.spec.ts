import { normalizeAfdagFilename, validateDagId } from '../ir';

// Document rename (PRD §6.1.8(A)) is filesystem-only; this validates/normalizes
// the user-entered name before `context.rename(...)`. The deploy-aware dag_id
// migration (§6.1.8(B)) is a separate follow-up and is not exercised here.
describe('normalizeAfdagFilename', () => {
  it('appends the .afdag extension when missing', () => {
    expect(normalizeAfdagFilename('sales_etl')).toEqual({
      name: 'sales_etl.afdag'
    });
  });

  it('keeps an existing .afdag extension (case-insensitive)', () => {
    expect(normalizeAfdagFilename('my_dag.afdag')).toEqual({
      name: 'my_dag.afdag'
    });
    expect(normalizeAfdagFilename('My_Dag.AFDAG')).toEqual({
      name: 'My_Dag.AFDAG'
    });
  });

  it('trims surrounding whitespace', () => {
    expect(normalizeAfdagFilename('  spaced  ')).toEqual({
      name: 'spaced.afdag'
    });
  });

  it('rejects an empty / whitespace-only name', () => {
    expect(normalizeAfdagFilename('')).toHaveProperty('error');
    expect(normalizeAfdagFilename('   ')).toHaveProperty('error');
  });

  it('rejects a path separator (rename is basename-only)', () => {
    expect(normalizeAfdagFilename('sub/dag')).toHaveProperty('error');
    expect(normalizeAfdagFilename('sub\\dag')).toHaveProperty('error');
  });
});

// dag_id change (PRD §6.1.8(B)) — the new id must be a valid Python identifier.
describe('validateDagId', () => {
  it('accepts a valid Python identifier (trimmed)', () => {
    expect(validateDagId('sales_etl')).toEqual({ id: 'sales_etl' });
    expect(validateDagId('  _job2  ')).toEqual({ id: '_job2' });
  });

  it('rejects an empty id', () => {
    expect(validateDagId('   ')).toHaveProperty('error');
  });

  it('rejects ids that are not valid identifiers', () => {
    expect(validateDagId('2bad')).toHaveProperty('error');
    expect(validateDagId('has space')).toHaveProperty('error');
    expect(validateDagId('dash-id')).toHaveProperty('error');
  });

  it('rejects Python keywords', () => {
    expect(validateDagId('class')).toHaveProperty('error');
    expect(validateDagId('return')).toHaveProperty('error');
  });
});
