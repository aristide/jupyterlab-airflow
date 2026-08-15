import { NodeResizer } from '@xyflow/react';
import type { NodeProps } from '@xyflow/react';
import * as React from 'react';

import { useEditorActions } from './editorContext';
import { useCanEdit } from './capabilitiesContext';

// An annotation note card (PRD §6.1.7): a resizable, free-text sticky note for
// team documentation. It has NO source/target handles, so it can never join a
// dependency edge, and it is split into the IR's separate `notes[]` array — so
// it never becomes an Airflow task. Resize is handled by NodeResizer (updates
// the node's width/height, persisted via flowToIR).
function NoteNodeImpl(props: NodeProps): JSX.Element {
  const data = props.data as { text?: unknown };
  const { updateNoteText, deleteNode } = useEditorActions();
  const canEdit = useCanEdit();

  return (
    <div className="jp-afdag-note">
      {/* Resize is an edit too — a viewer gets a fixed card. */}
      <NodeResizer
        minWidth={140}
        minHeight={70}
        isVisible={props.selected && canEdit}
      />
      {/* Drag handle: the textarea below has `nodrag` so it stays editable, so
          the note is repositioned by this bar (which does not). */}
      <div className="jp-afdag-note-bar">
        <span className="jp-afdag-note-grip" aria-hidden="true">
          ⋮⋮
        </span>
        {canEdit && (
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
        )}
      </div>
      <textarea
        className="jp-afdag-note-text nodrag nowheel"
        value={typeof data.text === 'string' ? data.text : ''}
        placeholder={canEdit ? 'Note for your team…' : ''}
        aria-label="Note text"
        readOnly={!canEdit}
        onChange={event => updateNoteText(props.id, event.target.value)}
      />
    </div>
  );
}

export const NoteNode = React.memo(NoteNodeImpl);
