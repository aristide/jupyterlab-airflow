import { keepWaitingPlan } from '../deployLifecycle';

// Regression: "Keep waiting" after the SERVER's deploy budget expired used to
// fall through to the legacy browser path. On the journaled path the widget
// deliberately holds no retire intent (`fallbackRetireRef` is nulled the moment
// the server says `reconciled: true`), so that fallback unpaused and triggered
// the renamed-TO dag while the renamed-FROM one was still on disk, registered
// and unpaused — two live DAGs running the same pipeline, which is exactly what
// the rename migration exists to prevent.
describe('keepWaitingPlan', () => {
  it('observes a journaled deploy that is still in flight', () => {
    expect(
      keepWaitingPlan({
        observedDeployId: 'd1',
        resumableDeployId: null,
        dagId: 'sales_v2',
        filename: 'sales_v2.py'
      })
    ).toEqual({ kind: 'observe', deployId: 'd1' });
  });

  it('resumes — never polls — a journaled deploy whose budget expired', () => {
    // `observedDeployId` is null here because `observeLifecycle` clears it on any
    // terminal outcome; that is precisely the state the old code mis-read as
    // "this deploy was never journaled".
    const plan = keepWaitingPlan({
      observedDeployId: null,
      resumableDeployId: 'd1',
      dagId: 'sales_v2',
      filename: 'sales_v2.py'
    });
    expect(plan).toEqual({ kind: 'resume', deployId: 'd1' });
    expect(plan.kind).not.toBe('poll');
  });

  it('polls only when no deploy was journaled at all (the fallback path)', () => {
    expect(
      keepWaitingPlan({
        observedDeployId: null,
        resumableDeployId: null,
        dagId: 'sales_v2',
        filename: 'sales_v2.py'
      })
    ).toEqual({ kind: 'poll', dagId: 'sales_v2', filename: 'sales_v2.py' });
  });

  it('does nothing when there is neither a deploy id nor a file to poll', () => {
    expect(
      keepWaitingPlan({ observedDeployId: null, resumableDeployId: null })
    ).toEqual({ kind: 'none' });
  });
});
