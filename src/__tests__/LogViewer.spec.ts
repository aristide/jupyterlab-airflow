import { classifyLine, levelFromStructured } from '../components/LogViewer';

describe('log line level classification (LogViewer)', () => {
  it('classifies the level token in an Airflow log line', () => {
    expect(
      classifyLine('[2026-06-15T17:25:01+0000] {ti.py:1} INFO - starting')
    ).toBe('info');
    expect(
      classifyLine('[2026-06-15T17:25:02+0000] {ti.py:9} ERROR - boom')
    ).toBe('error');
    expect(classifyLine('{x.py:2} WARNING - heads up')).toBe('warning');
    expect(classifyLine('{x.py:3} CRITICAL - fatal')).toBe('critical');
    expect(classifyLine('{x.py:4} DEBUG - noise')).toBe('debug');
  });

  it('treats a Python traceback as an error even without a level token', () => {
    expect(classifyLine('Traceback (most recent call last):')).toBe('error');
    expect(classifyLine('  File "/x/dag.py", line 12, in run')).toBe('error');
    expect(classifyLine('ValueError: bad input')).toBe('error');
    expect(classifyLine('KeyError: "missing"')).toBe('error');
  });

  it('falls back to plain for a line with no signal', () => {
    expect(classifyLine('just some output')).toBe('plain');
    expect(classifyLine('')).toBe('plain');
  });

  it('prefers the most severe token (critical over error)', () => {
    expect(classifyLine('CRITICAL ERROR happened')).toBe('critical');
  });

  it('matches the level in its Airflow position, not anywhere in the message', () => {
    // A benign INFO line that merely mentions ERROR/CRITICAL must stay info —
    // else it is painted red and steals the autoscroll-to-first-error target.
    expect(
      classifyLine(
        '[2026-06-15T17:25:01+0000] {ti.py:1} INFO - retrying after ERROR threshold'
      )
    ).toBe('info');
    expect(classifyLine('{ti.py:1} INFO - 0 ERROR rows')).toBe('info');
    expect(classifyLine('{ti.py:1} INFO - no CRITICAL issues found')).toBe(
      'info'
    );
    // A bare leading level token still classifies (and ignores trailing words).
    expect(classifyLine('INFO - configured log_level=ERROR')).toBe('info');
  });
});

describe('structured-event level mapping (levelFromStructured, PRD §6.6)', () => {
  it('maps Airflow level strings (any case) to a Level', () => {
    expect(levelFromStructured('INFO')).toBe('info');
    expect(levelFromStructured('info')).toBe('info');
    expect(levelFromStructured('warning')).toBe('warning');
    expect(levelFromStructured('WARN')).toBe('warning');
    expect(levelFromStructured('error')).toBe('error');
    expect(levelFromStructured('CRITICAL')).toBe('critical');
    expect(levelFromStructured('fatal')).toBe('critical');
    expect(levelFromStructured('debug')).toBe('debug');
  });

  it('returns null for an absent/unknown level so the caller falls back to text', () => {
    expect(levelFromStructured(undefined)).toBeNull();
    expect(levelFromStructured('')).toBeNull();
    expect(levelFromStructured('TRACE')).toBeNull();
  });
});
