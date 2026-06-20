import * as React from 'react';

import { IOperatorDef } from '../operators';

export interface IPaletteProps {
  operators: IOperatorDef[];
  onAdd: (id: string) => void;
  /** Add an annotation note card to the canvas. */
  onAddNote: () => void;
  /** Re-check which operators the target Airflow supports (PRD §6.2.1). */
  onRefresh?: () => void;
  /** Whether the panel is collapsed to a rail (canvas reclaims the width). */
  collapsed: boolean;
  /** Toggle the collapsed state. */
  onToggle: () => void;
}

// An operator whose provider isn't installed (or whose Airflow is too old) in
// the target. Shown but dimmed — never hidden, never blocked (PRD §6.2.1).
function isUnavailable(op: IOperatorDef): boolean {
  return (
    op.availability === 'missing-provider' ||
    op.availability === 'version-too-old'
  );
}

function unavailableHint(op: IOperatorDef): string {
  if (op.availability === 'version-too-old') {
    return `Needs Airflow ${op.airflowMinVersion ?? ''}+ — your Airflow is older. You can still add it to learn the shape.`;
  }
  const pip = op.pipInstall ?? `pip install ${op.provider ?? ''}`;
  return `Requires ${op.provider ?? 'a provider'} in your Airflow — ${pip}. You can still add it; deploy will block until it's installed.`;
}

/**
 * The searchable, category-grouped operator palette. Items are buttons
 * (click / keyboard to add a node) rather than drag-only, for accessibility.
 * Collapses to a thin rail with an expand affordance to give the canvas room.
 */
export function Palette(props: IPaletteProps): JSX.Element {
  const { operators, onAdd, onAddNote, onRefresh, collapsed, onToggle } = props;
  const [query, setQuery] = React.useState('');

  const groups = React.useMemo(() => {
    const map = new Map<string, IOperatorDef[]>();
    for (const op of operators.filter(o => matches(o, query))) {
      const list = map.get(op.category) ?? [];
      list.push(op);
      map.set(op.category, list);
    }
    return Array.from(map.entries());
  }, [operators, query]);

  if (collapsed) {
    return (
      <div className="jp-afdag-palette jp-mod-collapsed">
        <button
          className="jp-afdag-collapse-btn"
          title="Expand operators"
          aria-label="Expand operators panel"
          aria-expanded={false}
          onClick={onToggle}
        >
          »
        </button>
        <div className="jp-afdag-rail-label">Operators</div>
      </div>
    );
  }

  return (
    <div className="jp-afdag-palette">
      <div className="jp-afdag-palette-header">
        <span className="jp-afdag-palette-title">Operators</span>
        {onRefresh && (
          <button
            className="jp-afdag-palette-refresh"
            title="Re-check which operators your Airflow supports"
            aria-label="Refresh operator availability"
            onClick={onRefresh}
          >
            ⟳
          </button>
        )}
        <button
          className="jp-afdag-collapse-btn"
          title="Collapse operators"
          aria-label="Collapse operators panel"
          aria-expanded={true}
          onClick={onToggle}
        >
          «
        </button>
      </div>
      <input
        className="jp-afdag-search"
        placeholder="Search…"
        value={query}
        onChange={event => setQuery(event.target.value)}
      />
      <button
        className="jp-afdag-addnote-btn"
        title="Add a note card to the canvas"
        onClick={onAddNote}
      >
        + Add note
      </button>
      {groups.map(([category, items]) => (
        <div key={category} className="jp-afdag-palette-group">
          <div className="jp-afdag-palette-cat">{category}</div>
          {items.map(op => {
            const unavailable = isUnavailable(op);
            return (
              <button
                key={op.id}
                className={
                  unavailable
                    ? 'jp-afdag-palette-item jp-mod-unavailable'
                    : 'jp-afdag-palette-item'
                }
                title={unavailable ? unavailableHint(op) : `Add ${op.label}`}
                onClick={() => onAdd(op.id)}
              >
                <span className="jp-afdag-palette-item-label">{op.label}</span>
                {unavailable && (
                  <span
                    className="jp-afdag-palette-item-warn"
                    aria-label="not available in your Airflow"
                  >
                    ⓘ
                  </span>
                )}
              </button>
            );
          })}
        </div>
      ))}
    </div>
  );
}

function matches(op: IOperatorDef, query: string): boolean {
  const q = query.trim().toLowerCase();
  if (!q) {
    return true;
  }
  return (
    op.label.toLowerCase().includes(q) || op.category.toLowerCase().includes(q)
  );
}
