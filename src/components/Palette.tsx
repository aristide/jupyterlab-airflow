import * as React from 'react';

import { IOperatorDef } from '../operators';

export interface IPaletteProps {
  operators: IOperatorDef[];
  onAdd: (id: string) => void;
}

/**
 * The searchable, category-grouped operator palette. Items are buttons
 * (click / keyboard to add a node) rather than drag-only, for accessibility.
 */
export function Palette(props: IPaletteProps): JSX.Element {
  const { operators, onAdd } = props;
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

  return (
    <div className="jp-afdag-palette">
      <div className="jp-afdag-palette-title">Operators</div>
      <input
        className="jp-afdag-search"
        placeholder="Search…"
        value={query}
        onChange={event => setQuery(event.target.value)}
      />
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
