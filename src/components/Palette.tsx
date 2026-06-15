import * as React from 'react';

import { IOperatorDef } from '../operators';

export interface IPaletteProps {
  operators: IOperatorDef[];
  onAdd: (id: string) => void;
  /** Add an annotation note card to the canvas. */
  onAddNote: () => void;
  /** Whether the panel is collapsed to a rail (canvas reclaims the width). */
  collapsed: boolean;
  /** Toggle the collapsed state. */
  onToggle: () => void;
}

/**
 * The searchable, category-grouped operator palette. Items are buttons
 * (click / keyboard to add a node) rather than drag-only, for accessibility.
 * Collapses to a thin rail with an expand affordance to give the canvas room.
 */
export function Palette(props: IPaletteProps): JSX.Element {
  const { operators, onAdd, onAddNote, collapsed, onToggle } = props;
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
          {items.map(op => (
            <button
              key={op.id}
              className="jp-afdag-palette-item"
              title={`Add ${op.label}`}
              onClick={() => onAdd(op.id)}
            >
              {op.label}
            </button>
          ))}
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
