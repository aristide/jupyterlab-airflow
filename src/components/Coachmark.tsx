import * as React from 'react';

export type CoachStep = 1 | 2 | 3;

export interface ICoachmarkProps {
  step: CoachStep;
  /** Dismiss the tour (also from "Done" on the last step). */
  onSkip: () => void;
  /** Advance to the next step ("Next") or finish ("Done" on step 3). */
  onNext: () => void;
}

const STEPS: Array<{ title: string; body: string }> = [
  {
    title: 'Add your first task',
    body: 'Pick an operator from the Operators palette on the left — click it (or drag it onto the canvas) to add a task.'
  },
  {
    title: 'Configure the task',
    body: 'Select the task to open its form in the inspector on the right, and fill the required fields. A red ● marks a missing one.'
  },
  {
    title: 'Deploy to Airflow',
    body: 'When the badge reads ✓ no errors, click ▶ Deploy in the top bar to send the DAG to your Airflow.'
  }
];

/**
 * First-run onboarding coachmark (PRD §7 / §15.2): a small, dismissible
 * 3-step guide (add → configure → deploy) shown over the canvas for a new user.
 * Presentational — StudioApp advances the step from the graph/deploy state and
 * remembers dismissal so it doesn't re-appear.
 */
export function Coachmark(props: ICoachmarkProps): JSX.Element {
  const { step, onSkip, onNext } = props;
  const meta = STEPS[step - 1];
  return (
    // A passive, non-blocking advisory — a polite live region (not a focus-managed
    // dialog) so a screen reader announces the step text as it appears/changes.
    <div className="jp-afdag-coach" role="status" aria-live="polite">
      <div className="jp-afdag-coach-head">
        <span className="jp-afdag-coach-eyebrow">
          Getting started · Step {step} of 3
        </span>
        <button className="jp-afdag-coach-skip" onClick={onSkip}>
          Skip tour
        </button>
      </div>
      <div className="jp-afdag-coach-dots" aria-hidden="true">
        {[1, 2, 3].map(i => (
          <span key={i} className={i === step ? 'jp-mod-active' : ''} />
        ))}
      </div>
      <div className="jp-afdag-coach-title">{meta.title}</div>
      <p className="jp-afdag-coach-body">{meta.body}</p>
      {step > 1 && (
        <div className="jp-afdag-coach-actions">
          <button className="jp-afdag-btn" onClick={onNext}>
            {step === 3 ? 'Done' : 'Next →'}
          </button>
        </div>
      )}
    </div>
  );
}
