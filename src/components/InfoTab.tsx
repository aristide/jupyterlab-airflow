import * as React from 'react';

import { AfdagFlowNode } from '../graph';
import { IOperatorDef, IOperatorParam } from '../interfaces';
import { getOperator } from '../operators';

export interface IInfoTabProps {
  node: AfdagFlowNode | null;
}

// DAG-level concepts shown when no node is selected — Studio doubles as a way to
// learn Airflow (PRD §6.1.3 / §7).
const DAG_CONCEPTS: Array<{ term: string; explain: string }> = [
  {
    term: 'schedule',
    explain:
      'How often the DAG runs — a preset like @daily, a cron expression (0 9 * * *), or None for manual / triggered-only.'
  },
  {
    term: 'start_date',
    explain:
      'The first date the scheduler considers. A run is created for each schedule interval on or after it.'
  },
  {
    term: 'catchup',
    explain:
      'When on, Airflow back-fills every missed interval between start_date and now. Default off — most DAGs only run going forward.'
  },
  {
    term: 'retries / retry_delay',
    explain:
      'How many times a failed task is retried, and how long to wait between attempts.'
  }
];

function ParamList(props: {
  title: string;
  params: IOperatorParam[];
}): JSX.Element | null {
  if (props.params.length === 0) {
    return null;
  }
  return (
    <section className="jp-afdag-info-section">
      <div className="jp-afdag-info-heading">{props.title}</div>
      <dl className="jp-afdag-info-params">
        {props.params.map(param => (
          <React.Fragment key={param.name}>
            <dt>{param.label}</dt>
            {param.help ? <dd>{param.help}</dd> : <dd />}
          </React.Fragment>
        ))}
      </dl>
    </section>
  );
}

function OperatorInfo(props: { def: IOperatorDef }): JSX.Element {
  const { def } = props;
  const required = def.params.filter(p => p.required);
  const optional = def.params.filter(p => !p.required);
  return (
    <div className="jp-afdag-tabpanel jp-afdag-info">
      <div className="jp-afdag-info-cat">{def.category}</div>
      <h3 className="jp-afdag-info-title">{def.label}</h3>
      {def.description && (
        <p className="jp-afdag-info-desc">{def.description}</p>
      )}
      {(def.provider || def.airflowMinVersion) && (
        <div className="jp-afdag-info-meta">
          {def.provider && <span>Provider: {def.provider}</span>}
          {def.airflowMinVersion && (
            <span>Airflow {def.airflowMinVersion}+</span>
          )}
        </div>
      )}
      <ParamList title="Required inputs" params={required} />
      <ParamList title="Optional inputs" params={optional} />
      {def.example && (
        <section className="jp-afdag-info-section">
          <div className="jp-afdag-info-heading">Example</div>
          <pre className="jp-afdag-info-example">{def.example}</pre>
        </section>
      )}
      {def.docsUrl && (
        <a
          className="jp-afdag-info-docs"
          href={def.docsUrl}
          target="_blank"
          rel="noopener noreferrer"
        >
          Airflow docs ↗
        </a>
      )}
    </div>
  );
}

/**
 * INFO tab (PRD §6.1.3): read-only learning content about the selected
 * operator (description, required/optional inputs, provider, example, docs
 * link), or DAG-level concepts when nothing is selected. All text is registry
 * data rendered as plain text (React-escaped — never raw HTML), so a
 * user-supplied registry cannot inject markup.
 */
export function InfoTab(props: IInfoTabProps): JSX.Element {
  const def = props.node ? getOperator(props.node.data.op) : null;

  if (props.node && !def) {
    return (
      <div className="jp-afdag-tabpanel">
        <div className="jp-afdag-hint">
          Unknown operator: {props.node.data.op}
        </div>
      </div>
    );
  }

  if (def) {
    return <OperatorInfo def={def} />;
  }

  return (
    <div className="jp-afdag-tabpanel jp-afdag-info">
      <h3 className="jp-afdag-info-title">DAG basics</h3>
      <p className="jp-afdag-info-desc">
        Select a node to learn about its operator. Meanwhile, here are the core
        DAG settings (edited in the DAG tab):
      </p>
      <dl className="jp-afdag-info-params">
        {DAG_CONCEPTS.map(concept => (
          <React.Fragment key={concept.term}>
            <dt>{concept.term}</dt>
            <dd>{concept.explain}</dd>
          </React.Fragment>
        ))}
      </dl>
    </div>
  );
}
