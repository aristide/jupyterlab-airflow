import { defaultKeymap, history, historyKeymap } from '@codemirror/commands';
import { json } from '@codemirror/lang-json';
import { python } from '@codemirror/lang-python';
import { Compartment, EditorState } from '@codemirror/state';
import { EditorView, keymap, lineNumbers, placeholder } from '@codemirror/view';
import * as React from 'react';

export type CodeMirrorLanguage = 'python' | 'json';

export interface ICodeMirrorFieldProps {
  value: string;
  language: CodeMirrorLanguage;
  placeholder?: string;
  readOnly?: boolean;
  onChange?: (value: string) => void;
}

function languageExtension(language: CodeMirrorLanguage) {
  return language === 'python' ? python() : json();
}

/**
 * A thin React wrapper around a CodeMirror 6 editor (the same engine JupyterLab
 * uses) for `code`/`json` fields. The editor instance is created once per mount
 * and disposed on unmount; the language lives in a Compartment so it can be
 * reconfigured, and external `value` changes are reflected without resetting the
 * cursor when the text already matches.
 */
export function CodeMirrorField(props: ICodeMirrorFieldProps): JSX.Element {
  const { value, language, readOnly, onChange } = props;
  const hostRef = React.useRef<HTMLDivElement>(null);
  const viewRef = React.useRef<EditorView | null>(null);
  const langRef = React.useRef(new Compartment());
  const onChangeRef = React.useRef(onChange);
  onChangeRef.current = onChange;

  // Create the editor once.
  React.useEffect(() => {
    if (!hostRef.current) {
      return;
    }
    const state = EditorState.create({
      doc: value,
      extensions: [
        lineNumbers(),
        history(),
        keymap.of([...defaultKeymap, ...historyKeymap]),
        langRef.current.of(languageExtension(language)),
        EditorView.editable.of(!readOnly),
        EditorState.readOnly.of(Boolean(readOnly)),
        props.placeholder ? placeholder(props.placeholder) : [],
        EditorView.updateListener.of(update => {
          if (update.docChanged && onChangeRef.current) {
            onChangeRef.current(update.state.doc.toString());
          }
        })
      ]
    });
    const view = new EditorView({ state, parent: hostRef.current });
    viewRef.current = view;
    return () => {
      view.destroy();
      viewRef.current = null;
    };
    // Mount-once: language and value updates are handled by the effects below.
  }, []);

  // Reconfigure the language when it changes (e.g. switching selected node).
  React.useEffect(() => {
    viewRef.current?.dispatch({
      effects: langRef.current.reconfigure(languageExtension(language))
    });
  }, [language]);

  // Reflect external value changes (model reload, node switch) into the editor.
  React.useEffect(() => {
    const view = viewRef.current;
    if (view && value !== view.state.doc.toString()) {
      view.dispatch({
        changes: { from: 0, to: view.state.doc.length, insert: value }
      });
    }
  }, [value]);

  return <div className="jp-afdag-cm" ref={hostRef} />;
}
