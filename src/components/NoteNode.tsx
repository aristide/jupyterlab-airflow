import { NodeResizer } from '@xyflow/react';
import type { NodeProps } from '@xyflow/react';
import * as React from 'react';

import { useEditorActions } from './editorContext';

// An annotation note card (PRD §6.1.7): a resizable, free-text sticky note for
// team documentation. It has NO source/target handles, so it can never join a
// dependency edge, and it is split into the IR's separate `notes[]` array — so
// it never becomes an Airflow task. Resize is handled by NodeResizer (updates
// the node's width/height, persisted via flowToIR).
function NoteNodeImpl(props: NodeProps): JSX.Element {
  const data = props.data as { text?: unknown };
  const { updateNoteText, deleteNode } = useEditorActions();

  return (
    <div className="jp-afdag-note">
      <NodeResizer minWidth={140} minHeight={70} isVisible={props.selected} />
      {/* Drag handle: the textarea below has `nodrag` so it stays editable, so
          the note is repositioned by this bar (which does not). */}
      <div className="jp-afdag-note-bar">
        <span className="jp-afdag-note-grip" aria-hidden="true">
          ⋮⋮
        </span>
        <button
          className="jp-afdag-note-del nodrag nopan"
          title="Delete note"
          aria-label="Delete note"
          onClick={event => {
            event.stopPropagation();
            deleteNode(props.id);
          }}
        >
          ×
        </button>
      </div>
      <textarea
        className="jp-afdag-note-text nodrag nowheel"
        value={typeof data.text === 'string' ? data.text : ''}
        placeholder="Note for your team…"
        aria-label="Note text"
        onChange={event => updateNoteText(props.id, event.target.value)}
      />
    </div>
  );
}

export const NoteNode = React.memo(NoteNodeImpl);
